"""Read pool counts from a public Zcash compact-block service. Requires curl with HTTP/2."""

import argparse
import csv
import re
import struct
import subprocess
import tempfile
from collections import Counter
from pathlib import Path


COLUMNS = ["txid", "height", "block_hash", "index", "sapling_spends",
           "sapling_outputs", "orchard_actions", "ironwood_actions",
           "transparent_inputs", "transparent_outputs"]
METHOD = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange"


def varint(number):
    result = bytearray()
    while number >= 128:
        result.append(128 | (number & 127))
        number >>= 7
    result.append(number)
    return bytes(result)


def read_varint(data, pos):
    number = 0
    for shift in range(0, 70, 7):
        if pos >= len(data):
            raise ValueError("Truncated varint")
        byte = data[pos]
        pos += 1
        number |= (byte & 127) << shift
        if byte < 128:
            return number, pos
    raise ValueError("Varint exceeds 64 bits")


def length_field(tag, value):
    return varint(tag * 8 + 2) + varint(len(value)) + value


def fields(data):
    pos = 0
    while pos < len(data):
        key, pos = read_varint(data, pos)
        tag, wire_type = divmod(key, 8)
        if not tag:
            raise ValueError("Invalid protobuf field")
        if wire_type == 0:
            value, pos = read_varint(data, pos)
        else:
            if wire_type == 2:
                length, pos = read_varint(data, pos)
            elif wire_type in (1, 5):
                length = 8 if wire_type == 1 else 4
            else:
                raise ValueError(f"Unsupported protobuf wire type {wire_type}")
            end = pos + length
            if end > len(data):
                raise ValueError("Truncated protobuf field")
            value, pos = data[pos:end], end
        yield tag, value


def hex_id(raw):
    if len(raw) != 32:
        raise ValueError("Expected a 32-byte hash")
    return raw[::-1].hex()


def read_blocks(host, first, last):
    block_id = lambda height: b"\x08" + varint(height)
    request = length_field(1, block_id(first)) + length_field(2, block_id(last))
    # Explicitly request transparent, Sapling, Orchard and Ironwood transactions.
    request += length_field(3, bytes([1, 2, 3, 4]))
    framed = struct.pack(">BI", 0, len(request)) + request
    with tempfile.TemporaryDirectory() as temp:
        headers = Path(temp) / "headers"
        result = subprocess.run(
            ["curl", "-fsS", "--http2", "--max-time", "60", "-D", str(headers),
             "-H", "Content-Type: application/grpc", "-H", "TE: trailers",
             "--data-binary", "@-", "https://" + host + METHOD],
            input=framed, capture_output=True, check=True)
        if not re.search(r"^grpc-status:\s*0\s*$", headers.read_text(), re.I | re.M):
            raise ValueError("Server did not return successful gRPC trailers")
    data = result.stdout
    pos = 0
    expected = first
    while pos < len(data):
        if pos + 5 > len(data):
            raise ValueError("Truncated gRPC header")
        encoding, length = struct.unpack_from(">BI", data, pos)
        pos += 5
        if encoding or pos + length > len(data):
            raise ValueError("Compressed or truncated gRPC frame")
        frame = list(fields(data[pos:pos + length]))
        pos += length
        block = dict(frame)
        height = block[2]
        if height != expected or height > last:
            raise ValueError(f"Unexpected block height {height}, expected {expected}")
        expected += 1
        block_hash, previous = hex_id(block[3]), hex_id(block[4])
        rows = []
        for tag, transaction in frame:
            if tag != 7:
                continue
            parts = list(fields(transaction))
            tx = dict(parts)
            counts = Counter(tag for tag, _ in parts)
            rows.append([hex_id(tx[2]), height, block_hash, tx.get(1, 0),
                         counts[4], counts[5], counts[6], counts[9], counts[7], counts[8]])
        if not rows or [row[3] for row in rows] != list(range(len(rows))):
            raise ValueError(f"Missing or out-of-order transaction indexes at {height}")
        yield block_hash, previous, rows
    if expected != last + 1:
        raise ValueError("Server returned an incomplete block range")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="na.zec.rocks")
    parser.add_argument("--first", type=int, default=3495028)
    parser.add_argument("--last", type=int, default=3495127)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9.-]+(?::[0-9]+)?", args.host):
        parser.error("Supply a hostname with an optional port")
    if args.first < 1 or args.last < args.first:
        parser.error("Supply an inclusive positive block range")
    # Publish a completed file only after the final block and trailer checks.
    with tempfile.TemporaryDirectory(dir=args.output.parent) as temp:
        staged = Path(temp) / "result.csv"
        previous_hash = None
        with staged.open("w", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(COLUMNS)
            for first in range(args.first, args.last + 1, 300):
                for block_hash, previous, rows in read_blocks(
                        args.host, first, min(first + 299, args.last)):
                    if previous_hash is not None and previous != previous_hash:
                        raise ValueError("Block hashes break continuity")
                    writer.writerows(rows)
                    previous_hash = block_hash
        staged.replace(args.output)


if __name__ == "__main__":
    main()
