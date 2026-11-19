"""Replay the three transaction-file checks and the seven-day chart comparison."""

import argparse
import csv
import gzip
import hashlib
import io
import json
import re
import statistics
import sys
import tempfile
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

from rebuild_chain import COLUMNS, read_blocks

DATA = Path(__file__).resolve().parent / "data"
POOLS = COLUMNS[4:8]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def csv_snapshot(name):
    with gzip.open(DATA / name, "rt", newline="") as stream:
        return list(csv.DictReader(stream))


def reference(name):
    rows = csv_snapshot(name)
    indexed = {row["txid"]: row for row in rows}
    require(len(indexed) == len(rows), f"Repeated transactions in {name}")
    return indexed


def active(row):
    return any(int(row[k]) > 0 for k in POOLS)


def category(row):
    if int(row["index"]) == 0:
        return "coinbase"
    if not active(row):
        return "no_observed_pool"
    if int(row["transparent_inputs"]) or int(row["transparent_outputs"]):
        return "mixed"
    return "fully_shielded"


def activity_report(rows, flags):
    require(flags <= rows.keys(), "Export references unknown transactions")
    chain = {tid for tid, row in rows.items() if active(row)}
    return {"rows": len(rows), "on_chain": len(chain), "detected": len(chain & flags),
            "missed": len(chain - flags), "extra": len(flags - chain)}


def weekly_share(windows):
    blocks = csv_snapshot("october-blocks.csv.gz")
    require(len(blocks) == 8010, "Incomplete weekly block list")
    for prev, curr in zip(blocks, blocks[1:]):
        require(int(prev["height"]) + 1 == int(curr["height"]) and prev["hash"] == curr["previous"],
                "Weekly block hashes or heights do not connect")
    indexed = {int(block["height"]): block for block in blocks}
    shares = []
    for day in windows:
        interval = [indexed[i] for i in range(day["first"], day["last"] + 1)]
        denominator = sum(int(b["non_coinbase"]) for b in interval)
        numerator = sum(int(b["shielded_non_coinbase"]) for b in interval)
        require(denominator == day["non_coinbase_check"], f"Denominator changed for {day['date']}")
        shares.append(100 * numerator / denominator)
    return statistics.mean(shares)


def fetch(source):
    request = urllib.request.Request(source["url"], headers={"User-Agent": "Zcash-activity-replay/1"})
    with urllib.request.urlopen(request, timeout=50) as response:
        body = response.read()
    require(hashlib.sha256(body).hexdigest() == source["sha256"],
            "Source bytes changed. This historical comparison pins the version in data/manifest.json")
    return body


def check_chain(host, lo, hi, rows, require_all):
    seen = set()
    last_hash = None
    for block_hash, previous, transactions in read_blocks(host, lo, hi):
        require(last_hash is None or last_hash == previous, "Broken chain of block hashes")
        last_hash = block_hash
        for tx in transactions:
            txid = tx[0]
            if txid not in rows:
                require(not require_all, f"Additional network transaction {txid}")
                continue
            expected = rows[txid]
            require(txid not in seen and all(str(value) == expected[key] for key, value in zip(COLUMNS, tx)),
                    f"Network row differs for {txid}")
            seen.add(txid)
    require(seen == rows.keys(), "Network reply omitted a recorded transaction")


def public_research(body, rows, windows):
    source = json.loads(body)["windows"]
    require(source.keys() == windows.keys(), "Research windows changed")
    seen = set()
    for label, window in source.items():
        require(window["fully_shielded"] == windows[label]["fully_shielded"], "Published subtotal changed")
        for tx in window["raw_transactions"]:
            txid = tx["hash"]
            require(txid in rows and txid not in seen, "Missing or duplicate ID in research file")
            ref = rows[txid]
            require(ref["window"] == label and int(ref["height"]) == tx["block_id"], "Research window changed")
            require(bool(tx["is_coinbase"]) == (int(ref["index"]) == 0), "Coinbase marker changed")
            require(tx["is_coinbase"] or tx["input_count"] == int(ref["transparent_inputs"]),
                    "Research input count mismatch")
            require(tx["output_count"] == int(ref["transparent_outputs"]), "Research output count mismatch")
            seen.add(txid)
            yield {"research_window": label, **tx}, ref
    require(seen == rows.keys(), "Research source does not cover the original sample")


def bitquery_source(sources, rows, flags):
    try:
        import duckdb
    except ImportError as exc:
        raise ValueError("Install duckdb for Parquet verification using python -m pip install duckdb") from exc
    with tempfile.TemporaryDirectory() as temp, duckdb.connect() as db:
        def table(kind):
            local = Path(temp) / kind
            local.write_bytes(fetch(sources["bitquery_" + kind]))
            return db.execute("SELECT * FROM read_parquet(?)", [str(local)])
        cur = table("blocks")
        block_fields = [c[0] for c in cur.description]
        blocks = [dict(zip(block_fields, r)) for r in cur.fetchall()]
        expected_hashes = {int(r["height"]): r["block_hash"] for r in rows.values()}
        require(len(blocks) == len(expected_hashes)
                and {r["block"]: r["hash"] for r in blocks} == expected_hashes, "Source block hashes differ")
        cur = table("transactions")
        fields = [col[0] for col in cur.description]
        records = [dict(zip(fields, row)) for row in cur.fetchall()]
    seen = set()
    observed = set()
    for tx in records:
        txid = tx["tx_hash"]
        require(txid in rows and txid not in seen, "Missing or repeated Bitquery ID")
        ref = rows[txid]
        require(int(ref["height"]) == tx["block"] and int(ref["index"]) == tx["tx_index"],
                "Bitquery height or transaction index changed")
        seen.add(txid)
        if any(tx[k] for k in ("shielded_spent_count", "shielded_output_count", "joinsplit_count", "shielded_balance")):
            observed.add(txid)
        yield tx, ref
    require(seen == rows.keys() and observed == flags, "Bitquery coverage or field values changed")


def verify_blockchair(body, rows, flags):
    seen = set()
    observed = set()
    with gzip.open(io.BytesIO(body), "rt", newline="") as stream:
        for tx in csv.DictReader(stream, delimiter="\t"):
            txid = tx["hash"]
            require(txid in rows and txid not in seen and int(tx["block_id"]) == int(rows[txid]["height"]),
                    "Blockchair ID or height mismatch")
            seen.add(txid)
            if int(tx["shielded_value_delta"]) or any(json.loads(tx[k]) for k in
                    ("join_split_raw", "shielded_input_raw", "shielded_output_raw")):
                observed.add(txid)
    require(seen == rows.keys() and observed == flags, "Blockchair coverage or fields differ")


def corrected_rows(pairs):
    result = []
    for original, ref in pairs:
        preserved = {k: json.dumps(v, separators=(",", ":")) if isinstance(v, (list, dict)) else v
                     for k, v in original.items()}
        for name in ["block_hash", *POOLS]:
            preserved["chain_" + name] = ref[name]
        preserved["chain_structure"] = category(ref)
        result.append(preserved)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", action="store_true", help="Retrieve archived research JSON and two public Bitquery samples")
    parser.add_argument("--out", type=Path, help="With --sources, create a new directory containing two enriched CSVs")
    parser.add_argument("--chain-host", help="Independently replay the original 500 rows and the 100-block sample at this host")
    parser.add_argument("--november-file", type=Path, help="Verify a separately downloaded November 17 Blockchair TSV.gz")
    args = parser.parse_args()
    if args.out and (not args.sources or args.out.exists()):
        parser.error("--out requires --sources and a new output directory")
    if args.chain_host and not re.fullmatch(r"[a-zA-Z0-9.-]+(?::[0-9]+)?", args.chain_host):
        parser.error("--chain-host accepts a hostname with an optional port")
    manifest = json.loads((DATA / "manifest.json").read_text())
    for name, sha in manifest["files"].items():
        require(hashlib.sha256((DATA / name).read_bytes()).hexdigest() == sha, f"Snapshot changed: {name}")
    with gzip.open(DATA / "export-signals.json.gz", "rt") as f:
        signals = json.load(f)
    sample = reference("september-chain.csv.gz")
    research = reference("research-chain.csv.gz")
    november = reference("november-chain.csv.gz")
    sample_flags = set(signals["bitquery_nonzero_txids"])
    nov_flags = set(signals["blockchair_nonzero_txids"])
    windows = manifest["research_windows"]
    grouped = defaultdict(Counter)
    for row in research.values():
        grouped[row["window"]][category(row)] += 1
    published = sum(d["fully_shielded"] for d in windows.values())
    measured = sum(d["fully_shielded"] for d in grouped.values())
    require(len(research) == 500 and set(grouped) == set(windows) and (published, measured) == (10, 37),
            "Research classification differs")
    for label, counts in grouped.items():
        require(sum(counts.values()) == 100 and counts["coinbase"] == windows[label]["coinbase"],
                f"Research sample size or coinbase count differs for {label}")
    before, after = activity_report(sample, sample_flags), activity_report(november, nov_flags)
    require(before == {"rows": 580, "on_chain": 277, "detected": 16, "missed": 261, "extra": 0},
            "September result differs")
    require(after == {"rows": 9426, "on_chain": 3100, "detected": 3100, "missed": 0, "extra": 0},
            "November result differs")
    share = weekly_share(manifest["week"])
    require(round(share, 6) == 45.679659, "October daily mean differs")
    checks = ["offline snapshot"]
    if args.chain_host:
        check_chain(args.chain_host, 3495028, 3495127, sample, True)
        for label, window in windows.items():
            subset = {tid: row for tid, row in research.items() if row["window"] == label}
            check_chain(args.chain_host, window["first"], window["last"], subset, False)
        checks.append("fresh compact-block replay: " + args.chain_host)
    files = {}
    if args.sources:
        sources = manifest["sources"]
        files["september-bitquery-corrected.csv"] = corrected_rows(bitquery_source(sources, sample, sample_flags))
        files["archived-research-corrected.csv"] = corrected_rows(public_research(fetch(sources["research"]), research, windows))
        checks.append("original publisher files with verified SHA-256 and transaction coverage")
    if args.november_file:
        body = args.november_file.read_bytes()
        require(hashlib.sha256(body).hexdigest() == manifest["sources"]["blockchair_november"]["sha256"],
                "November file differs from recorded source")
        verify_blockchair(body, november, nov_flags)
        checks.append("original November 17 Blockchair export")
    if args.out:
        with tempfile.TemporaryDirectory(dir=args.out.parent) as temp:
            staged = Path(temp) / "output"
            staged.mkdir()
            for name, records in files.items():
                with (staged / name).open("w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=list(records[0]))
                    writer.writeheader()
                    writer.writerows(records)
            staged.rename(args.out)
    print(json.dumps({"result": "PASS", "checks": checks,
            "archived_research": {"rows": len(research), "published_fully_shielded": published,
                "chain_fully_shielded": measured, "non_coinbase": sum(category(r) != "coinbase" for r in research.values())},
            "bitquery_september": before, "blockchair_november": after,
            "zecscope_october_5": {"published_pct": manifest["published_weekly_share"],
                "chain_seven_day_mean_pct": round(share, 6)}}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"result": "FAIL", "reason": str(error)}), file=sys.stderr)
        sys.exit(1)
