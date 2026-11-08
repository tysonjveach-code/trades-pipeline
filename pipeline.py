"""
The full pipeline: CSV -> Python -> clean -> validate -> Postgres

Run it:
    python3 pipeline.py data/sample_trades.csv

Where everything ends up:
    Good + cleaned rows  -> trades table          (in the trades_db database)
    Bad rows             -> load_errors table     + logs/pipeline.log
    Duplicates           -> skipped               + logs/pipeline.log
    Summary of each run  -> load_runs table
"""

import hashlib
import logging
import os
import sys

import psycopg

from handlers import (
    handle_bad_row,
    handle_cleaned_row,
    handle_duplicate_row,
    handle_file_already_loaded,
    handle_good_row,
)
from load_csv import load_csv
from validate import clean_row, validate

# How to reach your database. Override it in the terminal if you use a password:
#   export DATABASE_URL="postgresql://postgres:yourpassword@localhost:5432/trades_db"
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/trades_db")

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(PROJECT_DIR, "logs", "pipeline.log")


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


def run(csv_path):
    setup_logging()
    file_name = os.path.basename(csv_path)
    digest = file_hash(csv_path)

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

            for row in rows:
                cleaned, was_changed = clean_row(row)
                errors = validate(cleaned)

                # ---- CASE: bad row ----
                if errors:
                    handle_bad_row(cur, row, run_id, errors)
                    counts["failed"] += 1
                    continue

                # Safety net: if your validate() misses something, Postgres will
                # reject it. conn.transaction() makes a "savepoint" so one bad
                # insert gets undone by itself instead of breaking the whole load.
                try:
                    with conn.transaction():
                        if was_changed:
                            inserted = handle_cleaned_row(cur, row, cleaned, run_id)
                        else:
                            inserted = handle_good_row(cur, cleaned, run_id)
                except (psycopg.errors.DataError, psycopg.errors.IntegrityError) as e:
                    reason = f"database rejected it: {e.diag.message_primary}"
                    handle_bad_row(cur, row, run_id, [reason])
                    counts["failed"] += 1
                    continue

                # ---- CASE: duplicate ----
                if not inserted:
                    handle_duplicate_row(cleaned)
                    counts["skipped"] += 1

                # ---- CASE: good or cleaned row ----
                elif was_changed:
                    counts["cleaned"] += 1
                else:
                    counts["loaded"] += 1

            cur.execute(
                """
                UPDATE load_runs
                SET rows_loaded = %s, rows_cleaned = %s, rows_failed = %s, rows_skipped = %s
                WHERE run_id = %s
                """,
                (counts["loaded"], counts["cleaned"], counts["failed"], counts["skipped"], run_id),
            )

    summary = (
        f"Run {run_id} done: {counts['loaded']} loaded, {counts['cleaned']} cleaned + loaded, "
        f"{counts['failed']} failed, {counts['skipped']} duplicates skipped"
    )
    logging.info(summary)
    print(summary)
    print(f"Details in {LOG_FILE}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 pipeline.py <path to csv>")
        sys.exit(1)

    run(sys.argv[1])
