"""
Step 1: Read a CSV file into Python.

The CSV is saved into a Python LIST of DICTIONARIES, in memory.
Each dictionary is one row, with the column names as keys.

Example: this CSV
    trade_id,symbol,side,price,quantity,trade_time
    T1,AAPL,BUY,189.50,100,2026-01-02 09:30:00

becomes this Python list:
    [
        {
            "row_number": 2,
            "trade_id": "T1",
            "symbol": "AAPL",
            "side": "BUY",
            "price": "189.50",       <- everything is a STRING at this point
            "quantity": "100",
            "trade_time": "2026-01-02 09:30:00",
        },
    ]

Nothing is in the database yet. The list only exists while the script runs.

Try it on its own:
    python3 load_csv.py data/sample_trades.csv
"""

import csv
import sys

EXPECTED_COLUMNS = ["trade_id", "symbol", "side", "price", "quantity", "trade_time"]


def load_csv(path):
    rows = []

    with open(path, newline="") as f:
        reader = csv.DictReader(f)

        # Make sure the header is what we expect before reading anything
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(
                f"CSV header is {reader.fieldnames}, expected {EXPECTED_COLUMNS}"
            )

        # start=2 because line 1 of the file is the header
        for row_number, row in enumerate(reader, start=2):
            row["row_number"] = row_number
            rows.append(row)

    return rows


if __name__ == "__main__":
    dataset = load_csv(sys.argv[1])

    print(f"Read {len(dataset)} rows into a list of dictionaries.\n")

    for row in dataset:
        print(row)
