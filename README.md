# Zcash Shielded Activity Checker

Use this prototype to reproduce two shielded-activity counts from public Zcash transaction exports. Each check matches the publisher's transaction IDs to records from Zcash blocks. The code handles the two archived source formats listed below.

## Run the offline check

Download or clone the files, open the directory containing `verify.py`, and run this with Python 3.10 or newer. The offline check uses the Python standard library and the snapshots in `data/`.

```sh
python verify.py
```

The command prints JSON. A successful run reports `"result": "PASS"`. A failed check reports `"result": "FAIL"` and exits with a nonzero status.

| Source | What the saved run verifies |
| --- | --- |
| ZecLedger's five 100-transaction research windows | The five published subtotals add up to 10 fully shielded transactions. The same rows contain 37 transactions with a shielded component and zero transparent inputs and outputs. Both counts exclude the same 105 mining rewards. |
| Bitquery's 580-transaction sample, blocks 3,495,028 through 3,495,127 | Four shielded fields in the export flagged 16 transactions. Public block records show 277 unique transactions touched Sapling, Orchard or Ironwood. All 261 additional transactions appear in the export with zeros in those fields. |

Bitquery's four tested fields are `shielded_spent_count`, `shielded_output_count`, `joinsplit_count` and `shielded_balance`. The counts measure visible transaction structure. Each transaction counts once even if it contains more than one shielded pool.

## Check the original files

To download the original files, verify their SHA-256 digests and write two corrected CSVs, run the following commands. Use a new output directory. Each CSV retains the publisher's original columns and adds the chain fields.

```sh
python -m pip install duckdb
python verify.py --sources --out corrected
```

To check the transaction details against a live public Zcash server, run either command below. These commands use curl with HTTP/2 support.

```sh
python verify.py --chain-host na.zec.rocks
python verify.py --chain-host zec-node.cakewallet.com
```

The two servers agreed on the block hashes, transaction IDs, positions, transparent input and output counts, and shielded component counts for the archived samples. The offline output also includes a November 17 Blockchair comparison and an October 5 ZecScope comparison. The cause of the ZecScope discrepancy remains unverified.

## Sources and license

- [ZecLedger research article](https://vancube2.medium.com/zcash-privacy-in-2026-4ae84968576b) and [archived 500-row file](https://github.com/vancube2/zecledger/blob/3f18a7883234ad910423983cb243d68774e62c75/research_data_500tx_verified.json)
- [Bitquery schema and samples](https://www.bitquery.io/datastore/datasets/zcash)
- [Zcash compact-block protocol](https://github.com/zcash/lightwallet-protocol/blob/main/walletrpc/compact_formats.proto)
- [Blockchair bulk-data documentation](https://blockchair.com/dumps) and [ZecScope's chart definition](https://zecscope.com/charts/shielded-tx-share)

The source URLs and file digests are recorded in `data/manifest.json`. The Python scripts are distributed under the included MIT license. The original publisher files are subject to their respective terms.
