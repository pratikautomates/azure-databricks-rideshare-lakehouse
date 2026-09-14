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

# Silver Layer

The Silver layer standardizes, validates, repairs, enriches, and prepares the Bronze rideshare data for downstream analytics.

The transformation logic is implemented in Azure Databricks using PySpark and writes the curated output in Delta format.

## Silver Processing Summary

Starting from:

```text
93,514,416 Bronze rows
```

the Silver pipeline:

- enforced explicit datatypes across all 21 source columns
- validated that datatype conversion introduced 0 new nulls
- identified and classified duration-related data-quality issues
- repaired 79 recoverable duration records using timestamp-derived values
- quarantined 5,000 unrecoverable records
- enriched pickup and dropoff community-area IDs using the 77-row Chicago Community Areas lookup
- added analytical features for duration, fare, speed, date, hour, weekday, peak periods, and distance bands
- retained trips with missing geographic or financial information using explicit data-quality flags
- wrote the curated output to Delta Lake
- partitioned the Silver dataset by `trip_month`

---

## Silver Row Reconciliation

```text
Bronze rows:      93,514,416
Silver rows:      93,509,416
Quarantine rows:       5,000
```

Validation:

```text
93,509,416
+    5,000
-----------
93,514,416
```

There was no unexplained row loss during the Bronze-to-Silver transformation.

---

## Duration Repair and Quarantine

Bronze profiling identified several duration-related issues.

### Recoverable Records

```text
79 rows
```

These records had:

- missing `trip_seconds`
- valid start and end timestamps
- positive timestamp-derived duration

For these rows, `trip_seconds` was reconstructed from the timestamps.

An explicit flag is retained:

```text
trip_seconds_imputed = true
```

### Quarantined Records

```text
5,000 rows
```

The quarantine breakdown is:

| Quarantine Reason | Rows |
|---|---:|
| `TRIP_END_BEFORE_TRIP_START` | 4,476 |
| `ZERO_OR_UNRECOVERABLE_DURATION` | 524 |
| **Total** | **5,000** |

These records were not silently dropped or artificially corrected.

They were written separately so they remain available for auditing and further investigation.

---

## Datatype Standardization

The Bronze API ingestion stored most fields as strings.

The Silver layer explicitly converts them into analytics-ready types, including:

- timestamps for trip start and end
- integer/long types for duration and categorical IDs
- double types for distance and coordinates
- decimal types for fare-related fields
- boolean types for shared-trip indicators

Casting validation returned:

```text
0 conversion failures
```

across all validated fields.

---

## Community Area Enrichment

The Silver layer enriches trips using the official Chicago Community Areas reference dataset.

The 77-row lookup is joined twice:

```text
pickup_community_area
        ↓
pickup_community_name
```

and:

```text
dropoff_community_area
        ↓
dropoff_community_name
```

Because the lookup is small, both joins use PySpark broadcast joins.

Validation confirmed:

```text
Unmatched non-null pickup community IDs:  0
Unmatched non-null dropoff community IDs: 0
```

Trips with missing community-area IDs are retained and labeled as:

```text
UNKNOWN
```

rather than being removed.

---

## Derived Analytical Features

The Silver layer creates additional analytical attributes including:

- `trip_duration_minutes`
- `fare_per_mile`
- `avg_speed_mph`
- `trip_date`
- `trip_hour`
- `trip_day_of_week`
- `trip_month`
- `is_weekend`
- `time_period`
- `is_peak_hour`
- `distance_category`
- `trip_seconds_imputed`
- geography quality flags
- financial-data quality flag

These columns prepare the dataset for Gold-layer aggregations and Power BI reporting.

---

## Final Silver Validation

The persisted Silver Delta dataset contains:

```text
Total Silver rows:           93,509,416
Null trip IDs:                        0
Imputed duration rows:               79
Invalid trip_seconds remaining:       0
Reversed timestamps remaining:        0
```

Geographic and financial quality indicators:

```text
Unknown pickup community:     16,577,458
Unknown dropoff community:    17,739,136
Rows with missing financial data: 409,138
```

---

## Delta Lake Output

The final Silver dataset is stored as Delta Lake at:

```text
lakehouse/silver/trips/
```

The quarantine dataset is stored separately at:

```text
lakehouse/silver/quarantine/trips/
```

The Silver table is partitioned by:

```text
trip_month
```

Post-write Delta metadata confirmed:

```text
Format: Delta
Partition column: trip_month
Number of files: 24
```

All 12 months of 2025 were successfully represented in the persisted Silver dataset.

---

## Silver Layer Status

**Completed and validated**

```text
93,509,416 clean Silver rows
5,000 quarantined records
79 repaired duration records
0 remaining invalid durations
77-area community lookup enrichment
Delta Lake output
12 monthly partitions
```

### Gold Layer

✅ Completed and validated

---

## Project Status

- ✅ Bronze Layer — Completed and validated
- ✅ Silver Layer — Completed and validated
- ✅ Gold Layer — Completed and validated
- 🚧 Power BI — In development
