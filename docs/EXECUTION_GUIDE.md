# Pipeline Execution Guide

## 1. Purpose

This guide explains how to run the cryptocurrency pipeline for:

1. A historical full load.
2. A standard CoinGecko incremental load.
3. A backfill using a selected historical file, folder, or processing date.

## 2. Recommended Notebook Parameters

The notebook should accept runtime parameters instead of requiring file paths to be changed throughout the code.

Add this parameter cell near the beginning of the notebook:

```python
dbutils.widgets.dropdown(
    "load_type",
    "incremental",
    ["full", "incremental", "backfill"]
)

dbutils.widgets.text(
    "source_path",
    "/Volumes/workspace/default/crypto_data/markets.json"
)

dbutils.widgets.text(
    "process_date",
    ""
)

load_type = dbutils.widgets.get("load_type")
source_path = dbutils.widgets.get("source_path")
process_date = dbutils.widgets.get("process_date")
```

Parameter meanings:

| Parameter | Example | Purpose |
|---|---|---|
| load_type | incremental | Selects full, incremental, or backfill processing |
| source_path | /Volumes/workspace/default/crypto_data/markets.json | Identifies the input file or folder |
| process_date | 2026-10-10 | Optional date used for a historical backfill |

## 3. Full Historical Load

Use the full load when creating or rebuilding the Bronze and historical Silver tables from the historical CSV files.

Example parameter values:

```text
load_type    = full
source_path  = /Volumes/workspace/default/crypto_data/historical_csv/
process_date =
```

The full-load branch can read the path as follows:

```python
if load_type == "full":
    historical_df = (
        spark.read
        .schema(crypto_schema)
        .option("header", "true")
        .csv(source_path)
    )
```

Full-load execution order:

```text
1. Capture start_time.
2. Read the historical CSV files with the explicit schema.
3. Add load_timestamp.
4. Write bronze_crypto_market.
5. Apply Silver transformations.
6. Remove duplicate coin and date records.
7. Write silver_crypto_market.
8. Capture end_time.
9. Append an execution record to pipeline_execution_logs.
```

The full-load section may use overwrite mode because its purpose is to rebuild the historical Bronze and Silver tables.

## 4. Standard Incremental Load

Use the incremental load when processing a new CoinGecko JSON snapshot.

Example parameter values:

```text
load_type    = incremental
source_path  = /Volumes/workspace/default/crypto_data/markets.json
process_date =
```

The incremental branch can read the input as follows:

```python
if load_type == "incremental":
    incremental_file = source_path.split("/")[-1]
    incremental_df = spark.read.json(source_path)
```

Standard incremental execution order:

```text
1. Capture start_time.
2. Read the CoinGecko JSON file from source_path.
3. Transform the CoinGecko fields into the Silver schema.
4. Overwrite incremental_silver_market with the current batch.
5. MERGE incremental_silver_market into silver_crypto_market.
6. Update rows with matching coin, quote currency, and date keys.
7. Insert rows with new coin, quote currency, and date keys.
8. Capture end_time.
9. Append an execution record to pipeline_execution_logs.
```

Do not rerun the historical Silver overwrite section during a normal incremental load. Otherwise, the previously merged CoinGecko records would be removed before the new merge.

## 5. Backfill Load

A backfill means reprocessing an older file, folder, batch, or processing date.

Use a backfill when:

- A historical file was missed.
- A previous batch failed.
- A correction is required for an older date.
- Historical Bronze or Silver records must be rebuilt.

### Backfill by File Path

Example parameter values:

```text
load_type    = backfill
source_path  = /Volumes/workspace/default/crypto_data/archive/markets_2026-10-05.json
process_date = 2026-10-05
```

The backfill branch can read the selected file as follows:

```python
if load_type == "backfill":
    backfill_df = spark.read.json(source_path)
```

The selected file is transformed into the Silver structure and merged by the same business key:

```text
coin_symbol + observation_date
```

If the coin and date already exist, the row is updated. If the coin and date do not exist, a new row is inserted.

### Backfill by Date-Based File Name

If files are stored using dates in their names, the path can be built from `process_date`:

```python
if load_type == "backfill" and process_date:
    source_path = (
        f"/Volumes/workspace/default/crypto_data/archive/"
        f"markets_{process_date}.json"
    )

    backfill_df = spark.read.json(source_path)
```

Example:

```text
process_date = 2026-10-05
```

This produces:

```text
/Volumes/workspace/default/crypto_data/archive/markets_2026-10-05.json
```

## 6. Suggested Load-Type Control

A simple control structure can be used in the notebook:

```python
if load_type == "full":
    print("Running full historical load")
    # Run Raw-to-Bronze and Bronze-to-Silver steps.

elif load_type == "incremental":
    print("Running standard incremental load")
    # Run CoinGecko staging and MERGE steps.

elif load_type == "backfill":
    print(f"Running backfill for: {source_path}")
    # Run the selected historical file through transformation and MERGE.

else:
    raise ValueError("Invalid load_type parameter")
```

## 7. Logging Each Run

Capture the start time before the selected pipeline branch begins:

```python
start_time = spark.sql(
    "SELECT current_timestamp()"
).first()[0]
```

Capture the end time after processing and merging finish:

```python
end_time = spark.sql(
    "SELECT current_timestamp()"
).first()[0]
```

The log should record:

```text
run_id
layer
file_processed
status
rows_processed
start_time
end_time
execution_time
```

The value for `file_processed` can be obtained automatically from the parameter:

```python
file_processed = source_path.split("/")[-1]
```

The log table must use append mode:

```python
log_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("pipeline_execution_logs")
```

## 8. Verification Queries

Check the main Silver row count:

```python
spark.sql("""
SELECT COUNT(*) AS total_rows
FROM silver_crypto_market
""").show()
```

Check the latest observation dates:

```python
spark.sql("""
SELECT observation_date, COUNT(*) AS record_count
FROM silver_crypto_market
GROUP BY observation_date
ORDER BY observation_date DESC
""").show(10, False)
```

Check a specific coin after a merge or backfill:

```python
spark.sql("""
SELECT coin_symbol, quote_currency, observation_date,
       close_price, volume, market_cap
FROM silver_crypto_market
WHERE coin_symbol = 'BTC'
ORDER BY observation_date DESC
""").show(10, False)
```

Check the execution log:

```python
spark.sql("""
SELECT *
FROM pipeline_execution_logs
ORDER BY execution_time DESC
""").show(truncate=False)
```

## 9. Important Execution Notes

- Run the full historical load only when the historical Bronze and Silver tables need to be created or rebuilt.
- Run only the incremental section for a normal CoinGecko update.
- Do not overwrite `silver_crypto_market` during a standard incremental run.
- Overwriting `incremental_silver_market` is acceptable because it is a staging table for the current batch.
- The MERGE operation modifies `silver_crypto_market`. It does not modify the source staging table.
- Processing the same CoinGecko date again should update matched rows instead of creating duplicate daily records.
- A new observation date should insert new daily records.
- Use the `source_path` or `process_date` parameter to select an older batch for backfill processing.
