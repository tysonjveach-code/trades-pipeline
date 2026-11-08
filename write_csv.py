"""
Write trades to a CSV in the exact format load_csv.py expects.

Usage:
    from write_csv import write_trades_csv

    trades = [
        {
            "trade_id": "T1",
            "symbol": "AAPL",
            "side": "BUY",
            "price": 189.5,
            "quantity": 100,
            "trade_time": datetime(2026, 1, 2, 9, 30, 0),
        },
    ]
    write_trades_csv("data/trades.csv", trades)
"""

import csv
from datetime import datetime

COLUMNS = ["trade_id", "symbol", "side", "price", "quantity", "trade_time"]


def format_value(column, value):
    """
    Turn one Python value into the text that goes in the CSV.

    Real numbers and dates get a consistent format.
    Strings are written exactly as given, so you can put bad
    values in on purpose ("abc", "", "-5") to test validation.

        format_value("price", 189.5)                          -> "189.50"
        format_value("trade_time", datetime(2026,1,2,9,30))  -> "2026-01-02 09:30:00"
        format_value("price", "abc")                          -> "abc"
    """
    if column == "price" and isinstance(value, float):
        return f"{value:.2f}"

    if column == "trade_time" and isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    return str(value)


def write_trades_csv(path, trades):
    """
    Write a header row, then one line per trade.

    `trades` is a list of dictionaries (or anything you can loop over).
    Each dictionary must have exactly the keys in COLUMNS.

    Returns the number of trades written.
    """
    count = 0

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()

        for trade in trades:
            missing = [column for column in COLUMNS if column not in trade]
            if missing:
                raise ValueError(f"trade {count + 1} is missing {missing}: {trade}")

            formatted = {}
            for column in COLUMNS:
                formatted[column] = format_value(column, trade[column])

            writer.writerow(formatted)
            count += 1

    return count
