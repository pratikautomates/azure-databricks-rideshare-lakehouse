## Azure Databricks Rideshare Analytics Lakehouse
End-to-end incremental batch data engineering pipeline using Azure Data Factory, ADLS Gen2, Azure Databricks, PySpark, Delta Lake, and Power BI.

## Tech Stack
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Pyspark
- Spark SQL
- Delta Lake
- Power BI

## Bronze Layer Status

Historical ingestion for 2025 is complete and validated.

- Total Bronze rows: 93,514,416
- Total columns: 21
- Months represented: 12/12
- Calendar dates represented: 365/365
- Date range: 2025-01-01 to 2025-12-31
- Duplicate trip IDs: 0

### Initial Data Quality Findings

- 4,725 rows with null `trip_seconds`
- 4,476 rows where `trip_end_timestamp` is earlier than `trip_start_timestamp`
- 354 rows with non-positive `trip_seconds`
- 0 rows with negative `trip_miles`
- 0 rows with negative `fare`
- 0 rows with negative `trip_total`
- 79 duration records are recoverable from start/end timestamps
- 5,000 duration/timestamp records identified for quarantine in the Silver layer

## Project Status
🚧 In Development
