"""
The full pipeline: CSV -> Python -> clean -> validate -> Postgres

Run it:
    python3 pipeline.py data/sample_trades.csv

Where everything ends up:
    Good + cleaned rows  -> trades table          (in the trades_db database)
    Bad rows             -> load_errors table     + logs/pipeline.log
    Duplicates           -> skipped               + logs/pipeline.log
    Summary of each run  -> load_runs table

All good rows are sent to Postgres at once with COPY (the fastest way).
If Postgres rejects anything, it falls back step by step so only the bad row fails:
    COPY everything -> batches of BATCH_SIZE -> one row at a time
"""

import hashlib
import logging
import os
import sys
import time

import psycopg

from handlers import (
    handle_bad_row,
    handle_duplicate_row,
    handle_file_already_loaded,
    handle_good_batch,
    handle_good_row,
    handle_good_rows_copy,
    log_cleaned_row,
)
from load_csv import load_csv
from validate import clean_row, validate

# How to reach your database. Override it in the terminal if you use a password:
#   export DATABASE_URL="postgresql://postgres:yourpassword@localhost:5432/trades_db"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/trades_db")

BATCH_SIZE = 4000

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(PROJECT_DIR, "logs", "pipeline.log")

# Errors that mean "Postgres didn't like the data", as opposed to
# "can't connect" or "the SQL is broken". Only these trigger the fallback.
BAD_DATA_ERRORS = (psycopg.errors.DataError, psycopg.errors.IntegrityError)


def setup_logging():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )


def file_hash(path):
    """A fingerprint of the file. Same contents -> same hash, every time."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def send_batch(conn, cur, batch, run_id, counts):
    """
    Try to insert the whole batch in one call.

    `batch` is a list of (original_row, cleaned_row) pairs. We keep the
    original so a bad row can be saved to load_errors exactly as it was.

    conn.transaction() makes a savepoint: if the batch fails, everything
    from this batch is undone, but earlier batches stay.
    """
    cleaned_rows = []
    for original, cleaned in batch:
        cleaned_rows.append(cleaned)

    try:
        with conn.transaction():
            inserted, duplicates = handle_good_batch(cur, cleaned_rows, run_id)
    except BAD_DATA_ERRORS as e:
        logging.warning(
            "BATCH rows %s-%s rejected (%s). Retrying one row at a time.",
            cleaned_rows[0]["row_number"],
            cleaned_rows[-1]["row_number"],
            e.diag.message_primary,
        )
        send_rows_one_at_a_time(conn, cur, batch, run_id, counts)
        return

    counts["loaded"] += inserted
    counts["skipped"] += duplicates


def send_rows_one_at_a_time(conn, cur, batch, run_id, counts):
    """
    The fallback. Slow, but only runs for a batch that had a bad row in it.
    Good rows still load, and the bad row goes to load_errors.
    """
    for original, cleaned in batch:
        try:
            with conn.transaction():
                inserted = handle_good_row(cur, cleaned, run_id)
        except BAD_DATA_ERRORS as e:
            reason = f"database rejected it: {e.diag.message_primary}"
            handle_bad_row(cur, original, run_id, [reason])
            counts["failed"] += 1
            continue

        if inserted:
            counts["loaded"] += 1
        else:
            handle_duplicate_row(cleaned)
            counts["skipped"] += 1


def send_with_copy(conn, cur, good_rows, run_id, counts):
    """
    Load ALL good rows at once with COPY (fastest).

    If Postgres rejects anything, undo it and fall back to batches.
    A bad batch then falls back to one row at a time, so only the bad row fails.
        COPY everything -> batches of 4000 -> one row at a time
    """
    if not good_rows:
        return

    cleaned_rows = []
    for original, cleaned in good_rows:
        cleaned_rows.append(cleaned)

    try:
        with conn.transaction():
            inserted, duplicates = handle_good_rows_copy(cur, cleaned_rows, run_id)
    except BAD_DATA_ERRORS as e:
        logging.warning("COPY rejected (%s). Falling back to batches.", e.diag.message_primary)
        for start in range(0, len(good_rows), BATCH_SIZE):
            batch = good_rows[start:start + BATCH_SIZE]
            send_batch(conn, cur, batch, run_id, counts)
        return

    counts["loaded"] += inserted
    counts["skipped"] += duplicates


def run(csv_path):
    setup_logging()
    start = time.perf_counter()

    file_name = os.path.basename(csv_path)
    digest = file_hash(csv_path)

    # loaded  = rows inserted into trades (includes cleaned ones)
    # cleaned = rows clean_row() had to fix
    counts = {"loaded": 0, "cleaned": 0, "failed": 0, "skipped": 0}

    # `with` = commit if everything works, roll back EVERYTHING if it crashes.
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:

            # ---- CASE: file already loaded ----
            cur.execute("SELECT run_id FROM load_runs WHERE file_hash = %s", (digest,))
            previous = cur.fetchone()
            if previous is not None:
                handle_file_already_loaded(file_name, previous[0])
                return

            # Start a new run and get its id
            cur.execute(
                "INSERT INTO load_runs (file_name, file_hash) VALUES (%s, %s) RETURNING run_id",
                (file_name, digest),
            )
            run_id = cur.fetchone()[0]
            logging.info("START run %s for %s", run_id, file_name)

            # Read the whole CSV into a list of dictionaries
            rows = load_csv(csv_path)

            good_rows = []
            seen_ids = set()

            for row in rows:
                cleaned, was_changed = clean_row(row)
                errors = validate(cleaned)

                # ---- CASE: bad row ----
                if errors:
                    handle_bad_row(cur, row, run_id, errors)
                    counts["failed"] += 1
                    continue

                # ---- CASE: duplicate row ----
                if cleaned["trade_id"] in seen_ids:
                    handle_duplicate_row(cleaned)
                    counts["skipped"] += 1
                    continue

                seen_ids.add(cleaned["trade_id"])

                # ---- CASE: messy but fixed ----
                if was_changed:
                    log_cleaned_row(row, cleaned)
                    counts["cleaned"] += 1

                # ---- CASE: good row -> collect it ----
                good_rows.append((row, cleaned))

            # ---- Every row has been checked. Now send all the good ones at once. ----
            send_with_copy(conn, cur, good_rows, run_id, counts)

            cur.execute(
                """
                UPDATE load_runs
                SET rows_loaded = %s, rows_cleaned = %s, rows_failed = %s, rows_skipped = %s
                WHERE run_id = %s
                """,
                (counts["loaded"], counts["cleaned"], counts["failed"], counts["skipped"], run_id),
            )

    seconds = time.perf_counter() - start
    summary = (
        f"Run {run_id} done in {seconds:.2f}s: {counts['loaded']} loaded "
        f"({counts['cleaned']} of them cleaned first), {counts['failed']} failed, "
        f"{counts['skipped']} duplicates skipped"
    )
    logging.info(summary)
    print(summary)
    print(f"Details in {LOG_FILE}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 pipeline.py <path to csv>")
        sys.exit(1)

    run(sys.argv[1])
