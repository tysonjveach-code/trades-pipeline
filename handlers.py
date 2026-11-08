"""
What to do in each case.

    Case                         Function                    Where it goes
    ---------------------------  --------------------------  -------------------------------
    File was already loaded      handle_file_already_loaded  nowhere, pipeline stops
    Many good rows at once       handle_good_batch           trades table (one call)
    Row is good                  handle_good_row             trades table
    Row was messy but fixed      handle_cleaned_row          trades table (cleaned version)
    Row is bad                   handle_bad_row              load_errors table + log file
    Row is already in the DB     handle_duplicate_row        nowhere, skipped + logged

Every function takes `cur` (the database cursor) so it can send SQL to Postgres.
"""

import json
import logging

logger = logging.getLogger("pipeline")


def handle_file_already_loaded(file_name, previous_run_id):
    """
    The exact same file (same SHA-256 hash) was loaded before.
    Do nothing, so nothing gets loaded twice.
    """
    logger.warning(
        "SKIPPED FILE %s: already loaded in run %s", file_name, previous_run_id
    )
    print(f"{file_name} was already loaded (run {previous_run_id}). Nothing to do.")


def handle_good_row(cur, row, run_id):
    """
    Insert one row into trades.

    Returns True if it was inserted.
    Returns False if a trade with that trade_id was already there.

    How we know: ON CONFLICT DO NOTHING skips duplicates, and RETURNING
    gives back the trade_id ONLY if a row was actually inserted.
        inserted   -> fetchone() gives ("T1",)
        duplicate  -> fetchone() gives None
    """
    cur.execute(
        """
        INSERT INTO trades (trade_id, symbol, side, price, quantity, trade_time, run_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO NOTHING
        RETURNING trade_id
        """,
        (
            row["trade_id"],
            row["symbol"],
            row["side"],
            row["price"],
            row["quantity"],
            row["trade_time"],
            run_id,
        ),
    )
    was_inserted = cur.fetchone() is not None

    if was_inserted:
        logger.info("LOADED row %s: %s", row["row_number"], row["trade_id"])

    return was_inserted


def handle_good_batch(cur, batch, run_id):
    """
    Insert a whole list of good rows in ONE call instead of one call per row.

    `batch` is a list of cleaned row dictionaries.
    Returns (inserted, duplicates).

    How we count duplicates: cur.rowcount is how many rows were actually
    inserted. Any row that wasn't inserted hit ON CONFLICT, so it's a duplicate.
        batch of 4000, rowcount 3612  ->  388 duplicates
    """
    values = []
    for row in batch:
        values.append((
            row["trade_id"],
            row["symbol"],
            row["side"],
            row["price"],
            row["quantity"],
            row["trade_time"],
            run_id,
        ))

    cur.executemany(
        """
        INSERT INTO trades (trade_id, symbol, side, price, quantity, trade_time, run_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO NOTHING
        """,
        values,
    )

    inserted = cur.rowcount
    duplicates = len(batch) - inserted

    first_row = batch[0]["row_number"]
    last_row = batch[-1]["row_number"]
    logger.info(
        "BATCH rows %s-%s: %s inserted, %s duplicates skipped",
        first_row, last_row, inserted, duplicates,
    )

    return inserted, duplicates


def log_cleaned_row(original_row, cleaned_row):
    """Write down exactly what clean_row() changed."""
    changes = []
    for column in cleaned_row:
        if original_row.get(column) != cleaned_row[column]:
            changes.append(f"{column}: {original_row.get(column)!r} -> {cleaned_row[column]!r}")

    logger.info("CLEANED row %s: %s", cleaned_row["row_number"], ", ".join(changes))


def handle_cleaned_row(cur, original_row, cleaned_row, run_id):
    """
    The row had small formatting problems that clean_row() fixed.
    Log what changed, then insert the cleaned version like a good row.
    """
    log_cleaned_row(original_row, cleaned_row)
    return handle_good_row(cur, cleaned_row, run_id)


def handle_bad_row(cur, row, run_id, errors):
    """
    The row failed validation. Save it to load_errors with the reasons,
    so someone can look at it and fix it later.
    """
    raw_row = {key: value for key, value in row.items() if key != "row_number"}
    reason = "; ".join(errors)

    cur.execute(
        """
        INSERT INTO load_errors (run_id, row_number, raw_row, reason)
        VALUES (%s, %s, %s, %s)
        """,
        (run_id, row["row_number"], json.dumps(raw_row), reason),
    )

    logger.error("FAILED row %s: %s", row["row_number"], reason)


def handle_duplicate_row(row):
    """
    The row was fine, but a trade with this trade_id is already in the database.
    Nothing to insert. Just record that it happened.
    """
    logger.warning(
        "DUPLICATE row %s: trade_id %s already in trades, skipped",
        row["row_number"],
        row["trade_id"],
    )
