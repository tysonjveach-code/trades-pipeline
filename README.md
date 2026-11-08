# Trades Pipeline

Loads trade records from a CSV into PostgreSQL. Each row is cleaned, validated, and sent to the right place. Duplicates and repeat loads are blocked, and every failure is logged.

## How each row is handled

| Case | Example | Where it goes |
|---|---|---|
| Good | `T1,AAPL,BUY,189.50,...` | `trades` table |
| Messy but fixable | `  nvda ` → `NVDA`, `buy` → `BUY` | cleaned, then `trades` |
| Bad | price `abc`, quantity `0`, side `HOLD` | `load_errors` table + `logs/pipeline.log` |
| Duplicate trade | `T2` appears twice | skipped + logged |
| Same file loaded again | run the script twice | whole file skipped |

## Files

- `load_csv.py`: reads the CSV into a list of dictionaries
- `validate.py`: `clean_row()` fixes formatting, `validate()` rejects bad rows
- `handlers.py`: one function per case above
- `pipeline.py`: ties it all together
- `sql/schema.sql`: creates the tables
- `data/sample_trades.csv`: test file with one of every case

## Setup (Mac)

```
brew install postgresql@16
brew services start postgresql@16
createdb trades_db
psql -d trades_db -f sql/schema.sql
pip3 install -r requirements.txt
```

If your Postgres needs a username and password:

```
export DATABASE_URL="postgresql://postgres:yourpassword@localhost:5432/trades_db"
```

## Run

```
python3 pipeline.py data/sample_trades.csv
```

Run it a second time to see the duplicate-file check stop it.

## Check the results

```
psql -d trades_db -c "SELECT * FROM trades;"
psql -d trades_db -c "SELECT row_number, reason FROM load_errors;"
psql -d trades_db -c "SELECT * FROM load_runs;"
```

## Start over

```
psql -d trades_db -c "TRUNCATE trades, load_errors, load_runs RESTART IDENTITY;"
```
