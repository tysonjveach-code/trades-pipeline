"""
Step 2 and 3: clean_row() and validate()

These are two DIFFERENT jobs:

  clean_row()  -> FIXES small, harmless mess so the row can be used.
                  "  nvda " -> "NVDA",  "buy" -> "BUY",  " 178.10 " -> "178.10"
                  The data was right, it was just formatted badly.

  validate()   -> REJECTS rows that are actually wrong.
                  price "abc", quantity 0, side "HOLD", missing symbol.
                  You can't guess what the right value was, so you don't fix it.

The pipeline always calls clean_row() first, then validate().
"""

from datetime import datetime


def clean_row(row):
    """
    Returns (cleaned_row, was_changed).

    Example:
        input:  {"symbol": "  nvda ", "side": "buy", ...}
        output: ({"symbol": "NVDA", "side": "BUY", ...}, True)
    """
    cleaned = dict(row)

    for column in ["trade_id", "symbol", "side", "price", "quantity", "trade_time"]:
        value = cleaned.get(column)
        if value is not None:
            cleaned[column] = value.strip()

    if cleaned.get("symbol") is not None:
        cleaned["symbol"] = cleaned["symbol"].upper()

    if cleaned.get("side") is not None:
        cleaned["side"] = cleaned["side"].upper()

    was_changed = cleaned != row
    return cleaned, was_changed


def validate(row):
    """
    YOUR FUNCTION TO WRITE.

    Takes one cleaned row (a dictionary of strings).
    Returns a LIST of problems:
        []                          -> row is good
        ["price must be > 0"]       -> row is bad, here's why
        ["missing symbol", "..."]   -> can have more than one problem

    One check is done below as an example. Add the rest the same way.
    """
    errors = []

    # A row with too few columns has None for the missing values.
    # Check this first so the checks below don't crash on None.
    for column, value in row.items():
        if value is None:
            errors.append(f"missing column: {column}")
    if errors:
        return errors

    try:
        price = float(row["price"])
        if price <= 0:
            errors.append(f"price must be > 0, got {row['price']!r}")
    except ValueError:
        errors.append(f"price is not a number: {row['price']!r}")

    if row["trade_id"] == "":
      errors.append("trade_id is empty")
      
    try:
        if not row["symbol"].isalpha():
            errors.append("Symbol might be empty or contains characters other than letters")
    except ValueError:
        errors.append(f"symbol is not of the right format {row['symbol']!r}")
    
    try:
        if row["side"] not in ("BUY", "SELL"):
            errors.append('Side might be empty or doesnt contain "BUY" or "SELL"')
    except ValueError:
        errors.append(f"Side is not of the right format {row['side']!r}")

    try:
        quantity = int(row["quantity"])
        if quantity <= 0:
            errors.append('Quantity might be whole and be greater than 0')
    except ValueError:
        errors.append(f"Quantity is not of the right format {row['quantity']!r}")

    try:
        datetime.fromisoformat(row["trade_time"])
    except ValueError:
        errors.append(f"Time is not of the right format {row['trade_time']!r}")

    return errors
