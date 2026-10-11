Cryptocurrency Data Pipeline(24i-2516 and 24L-2517)
1. Project Overview
This project implements a cryptocurrency data pipeline in Databricks using PySpark and Delta Lake.
The pipeline processes two types of data:
Historical cryptocurrency CSV files used for the full load.
CoinGecko `markets.json` files used for incremental loads.
The historical records are first loaded into a Bronze Delta table and then cleaned and transformed into a Silver Delta table. CoinGecko records are transformed into the same Silver structure and merged into the main Silver table by using an upsert operation.
2. Pipeline Flow
Historical data flow:
```text
Historical CSV Files
        |
        v
Bronze DataFrame
        |
        v
bronze\\\_crypto\\\_market
        |
        v
Silver Transformations
        |
        v
silver\\\_crypto\\\_market
```
Incremental data flow:
```text
CoinGecko markets.json
        |
        v
incremental\\\_df
        |
        v
incremental\\\_silver\\\_df
        |
        v
incremental\\\_silver\\\_market
        |
        v
MERGE INTO silver\\\_crypto\\\_market
```
Operational logging flow:
```text
Pipeline Run
        |
        v
pipeline\\\_execution\\\_logs
```
3. Data Grain
The grain of the main Silver table is:
```text
One cryptocurrency in one quote currency on one observation date
```
The business key used for incremental merging is:
```text
coin\\\_symbol + observation\\\_date
```
For the current CoinGecko load, the quote currency is USD.
4. Bronze Data Model
Table name:
```text
bronze\\\_crypto\\\_market
```
Purpose:
The Bronze table stores the historical CSV data with minimal changes. It preserves the original source columns and adds an ingestion timestamp.
Column	Data Type	Description
high	DOUBLE	Highest recorded price in the source period
open	DOUBLE	Opening price in the source period
volume	DOUBLE	Trading volume in the source period
close	DOUBLE	Closing price in the source period
low	DOUBLE	Lowest recorded price in the source period
timestamp	LONG	Unix timestamp from the historical source
ticker	STRING	Trading pair such as BTC-USD
load_timestamp	TIMESTAMP	Time when the record was loaded into Bronze
The historical CSV files are read with an explicit `StructType` schema to avoid relying only on automatic type inference.
5. Silver Data Model
Main table name:
```text
silver\\\_crypto\\\_market
```
Purpose:
The Silver table stores cleaned and standardized cryptocurrency market records. It contains historical records and the merged CoinGecko incremental records.
Column	Data Type	Description
coin_symbol	STRING	Cryptocurrency symbol in uppercase, such as BTC
quote_currency	STRING	Quote currency, such as USD
observation_date	DATE	Business date of the market observation
open_price	DOUBLE	Opening price. This may be null for CoinGecko snapshot records
high_price	DOUBLE	Historical high price or CoinGecko 24-hour high
low_price	DOUBLE	Historical low price or CoinGecko 24-hour low
close_price	DOUBLE	Historical closing price or CoinGecko current price
volume	DOUBLE	Historical volume or CoinGecko total volume
load_timestamp	TIMESTAMP	Time when the record was processed
market_cap	DOUBLE	Market capitalization. This is null for historical rows that do not provide it
Historical transformation rules:
```text
ticker              -> coin\\\_symbol and quote\\\_currency
timestamp           -> observation\\\_date
open                -> open\\\_price
high                -> high\\\_price
low                 -> low\\\_price
close               -> close\\\_price
volume              -> volume
market\\\_cap          -> NULL
```
CoinGecko transformation rules:
```text
symbol              -> coin\\\_symbol using uppercase
USD                 -> quote\\\_currency
last\\\_updated        -> observation\\\_date
NULL                -> open\\\_price
high\\\_24h            -> high\\\_price
low\\\_24h             -> low\\\_price
current\\\_price       -> close\\\_price
total\\\_volume        -> volume
market\\\_cap          -> market\\\_cap
current timestamp   -> load\\\_timestamp
```
6. Incremental Silver Staging Model
Table name:
```text
incremental\\\_silver\\\_market
```
Purpose:
This Delta table stores the latest transformed CoinGecko batch before it is merged into the main Silver table. Its columns follow the same structure as `silver\\\_crypto\\\_market`.
The staging table is overwritten for each new incremental batch. The main Silver table is not overwritten during an incremental run.
7. Merge and Idempotency
The incremental table is merged into the main Silver table by using this matching condition:
```text
coin\\\_symbol + observation\\\_date
```
The merge behavior is:
```text
Matched record     -> UPDATE
Unmatched record   -> INSERT
```
This prevents duplicate daily records when the same CoinGecko file is processed more than once. A newer snapshot from the same date updates the existing row. A snapshot from a new date inserts a new row.
8. Schema Drift Handling
Delta Lake schema evolution is used when a new column must be added to the Silver model.
The `market\\\_cap` column was added to support the CoinGecko source even though it was not present in the historical CSV files. The historical rows receive a nullable `market\\\_cap` field, and the Silver Delta table is written with schema evolution enabled.
```python
.option("mergeSchema", "true")
```
The selected CoinGecko columns are also explicitly cast to the expected Silver data types before the merge.
9. Operational Logging Model
Table name:
```text
pipeline\\\_execution\\\_logs
```
Purpose:
The logging table stores operational information about pipeline execution.
Column	Data Type	Description
run_id	BIGINT	Unique identifier created for the pipeline run
layer	STRING	Pipeline stage being processed
file_processed	STRING	File or parameter used by the run
status	STRING	SUCCESS or FAILURE
rows_processed	BIGINT	Number of source rows processed in the run
start_time	TIMESTAMP	Pipeline execution start time
end_time	TIMESTAMP	Pipeline execution end time
execution_time	TIMESTAMP	Time when the log entry was written
The logging table uses append mode so that previous execution records are preserved.
10. Main Delta Tables
```text
bronze\\\_crypto\\\_market
silver\\\_crypto\\\_market
incremental\\\_silver\\\_market
pipeline\\\_execution\\\_logs
```
11. Technologies Used
Databricks
PySpark
Spark SQL
Delta Lake
CoinGecko market data
Historical cryptocurrency CSV files
