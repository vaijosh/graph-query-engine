#!/usr/bin/env python3
"""
Normalize Kaggle IBM AML CSV to Graph Query Engine loader format.
Usage: python3 scripts/normalize_aml.py [--src demo/data/HI-Small_Trans.csv] [--rows 100000]
"""
import csv
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="demo/data/HI-Small_Trans.csv")
    parser.add_argument("--dst", default="demo/data/aml-demo.csv")
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()

    src = Path(args.src)
    dst = Path(args.dst)

    if not src.exists():
        print(f"ERROR: Source file not found: {src}")
        return

    count = 0
    laundering = 0

    print(f"Reading from: {src}")
    print(f"Writing to:   {dst}")
    print(f"Max rows:     {args.rows}")
    print()

    with src.open(newline="", encoding="utf-8", errors="ignore") as fin, \
         dst.open("w", newline="", encoding="utf-8") as fout:

        reader = csv.reader(fin)
        raw_headers = next(reader)
        print(f"Raw headers: {raw_headers}")

        # Kaggle AML schema (positional):
        # 0:Timestamp  1:From Bank  2:Account(from)  3:To Bank  4:Account(to)
        # 5:Amount Received  6:Receiving Currency  7:Amount Paid
        # 8:Payment Currency  9:Payment Format  10:Is Laundering
        fout.write("from_bank,from_account,to_bank,to_account,amount_paid,payment_currency,payment_format,timestamp,is_laundering,transaction_id\n")

        for i, row in enumerate(reader):
            if i >= args.rows:
                break
            if len(row) < 11:
                continue
            from_bank     = row[1].strip()
            from_account  = row[2].strip()
            to_bank       = row[3].strip()
            to_account    = row[4].strip()
            amount_paid   = row[7].strip()
            pay_currency  = row[8].strip()
            pay_format    = row[9].strip()
            timestamp     = row[0].strip()
            is_laundering = row[10].strip()
            tx_id         = str(i + 1)

            fout.write(f"{from_bank},{from_account},{to_bank},{to_account},{amount_paid},{pay_currency},{pay_format},{timestamp},{is_laundering},{tx_id}\n")

            if is_laundering == "1":
                laundering += 1
            count += 1

    print()
    print(f"Done!")
    print(f"  Rows written:    {count}")
    print(f"  Suspicious rows: {laundering} ({laundering/max(count,1)*100:.2f}%)")
    print(f"  Clean rows:      {count - laundering}")
    print()
    print(f"Ready to load:")
    print(f'  curl -X POST "http://localhost:7000/admin/load-aml-csv?path=$(pwd)/{dst}&maxRows={count}"')

if __name__ == "__main__":
    main()

