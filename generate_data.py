"""
Make a CSV of random trades, with some wrong rows on purpose.

Run it:
    python3 generate_data.py
"""

import random
from datetime import datetime, timedelta

from write_csv import write_trades_csv

NUM_TRADES = 1_000_000          # change to 1_000_000 for the performance test later
WRONG_ROW_CHANCE = 0.20     # 20% of rows are wrong: invalid data OR a duplicate
MESSY_ROW_CHANCE = 0.05     # 5% get messy formatting that clean_row() should fix
OUTPUT_FILE = "data/trades.csv"

SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "TSLA"]
START_TIME = datetime(2026, 1, 2, 9, 30, 0)


def add_invalid_data(trade):
    """Break one field so validate() should reject the row."""
    problem = random.choice([
        "bad_price",
        "negative_price",
        "empty_symbol",
        "symbol_with_numbers",
        "bad_side",
        "zero_quantity",
        "decimal_quantity",
        "bad_date",
    ])

    if problem == "bad_price":
        trade["price"] = "abc"
    elif problem == "negative_price":
        trade["price"] = "-5"
    elif problem == "empty_symbol":
        trade["symbol"] = ""
    elif problem == "symbol_with_numbers":
        trade["symbol"] = "AA1"
    elif problem == "bad_side":
        trade["side"] = "HOLD"
    elif problem == "zero_quantity":
        trade["quantity"] = 0
    elif problem == "decimal_quantity":
        trade["quantity"] = "2.5"
    elif problem == "bad_date":
        trade["trade_time"] = "not-a-date"

    return trade


def make_messy(trade):
    """Not wrong, just badly formatted. clean_row() should fix it."""
    trade["symbol"] = "  " + trade["symbol"].lower() + " "
    trade["side"] = trade["side"].lower()
    return trade


trades = []
counts = {"good": 0, "invalid": 0, "duplicate": 0, "messy": 0}

for i in range(NUM_TRADES):
    # New values for these variables on every pass through the loop
    trade_id = f"T{i}"
    symbol = random.choice(SYMBOLS)
    side = random.choice(["BUY", "SELL"])
    price = round(random.uniform(50, 500), 2)
    quantity = random.randint(1, 1000)
    trade_time = START_TIME + timedelta(seconds=i)

    trade = {
        "trade_id": trade_id,
        "symbol": symbol,
        "side": side,
        "price": price,
        "quantity": quantity,
        "trade_time": trade_time,
    }

    if random.random() < WRONG_ROW_CHANCE:
        # This row is wrong. Pick which kind (a duplicate needs an earlier row).
        if i > 0 and random.random() < 0.5:
            earlier = random.choice(trades)
            trade = dict(earlier)
            counts["duplicate"] += 1
        else:
            trade = add_invalid_data(trade)
            counts["invalid"] += 1

    elif random.random() < MESSY_ROW_CHANCE:
        trade = make_messy(trade)
        counts["messy"] += 1

    else:
        counts["good"] += 1

    trades.append(trade)

count = write_trades_csv(OUTPUT_FILE, trades)

wrong = counts["invalid"] + counts["duplicate"]
print(f"Wrote {count} trades to {OUTPUT_FILE}")
print(f"  good:      {counts['good']}")
print(f"  messy:     {counts['messy']}  (should be cleaned and loaded)")
print(f"  invalid:   {counts['invalid']}")
print(f"  duplicate: {counts['duplicate']}")
print(f"  wrong total: {wrong} ({wrong / count:.1%})")
