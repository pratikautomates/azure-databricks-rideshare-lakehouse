# Troubleshooting & Engineering Decisions

This document captures the major technical issues encountered while building the Azure Databricks Rideshare Analytics Lakehouse, along with the debugging approach, resolution, and engineering lessons from each problem.

---

## 1. Parameterized REST Request Returned `400 Bad Request`

### Problem
The initial Azure Data Factory pipeline worked for a small sample request, but the parameterized Chicago rideshare API request failed with a `400 Bad Request`.

The error was initially surfaced through ADF as a hierarchical-to-tabular/schema-related failure, which made the issue appear to be related to Parquet conversion.

### Root Cause
The actual problem was the construction of the Socrata API request.

The original approach attempted to pass a more complex filtered query using the GET-style request pattern. This became unreliable once date filters, ordering, and pagination were introduced.

### Resolution
The source request was changed to:

- HTTP method: `POST`
- SoQL query passed inside the JSON request body
- `X-App-Token` supplied through request headers
- `Content-Type: application/json`

This made the API request easier to parameterize and allowed date filters and pagination values to be injected dynamically.

### Key Learning
ADF error messages can sometimes surface failures from later pipeline stages even when the real issue occurs at the source request. Testing the API request independently helped isolate the problem.

---

## 2. Floating Timestamp Type Mismatch

### Problem

The row-count Web activity returned an error similar to:

```text
Type mismatch: expected floating_timestamp, but found text
```

The generated request contained timestamps with a timezone suffix:

```text
2025-02-01T00:00:00Z
```

### Root Cause

The Socrata `trip_start_timestamp` field uses a floating timestamp datatype.

The additional `Z` suffix caused the timestamp value to be interpreted incorrectly by the API.

### Resolution

The pipeline defensively removed the `Z` suffix using ADF expressions:

```text
replace(pipeline().parameters.p_start_date,'Z','')
```

and:

```text
replace(pipeline().parameters.p_end_date,'Z','')
```

This produced timestamps such as:

```text
2025-02-01T00:00:00
```

### Result

The row-count Web activity successfully returned the expected source counts.

### Key Learning

Timestamp formatting must match the datatype expected by the source system.

Small formatting differences can cause otherwise valid API queries to fail.

---

## 3. Initial `Until` Pagination Design Failed

### Problem

The first pagination implementation used an Azure Data Factory `Until` loop with variables controlling the current page number.

January 2025 contained:

```text
Rows: 7,607,290
Page size: 50,000
Pages required: 153
```

Although the pipeline validated successfully, execution repeatedly failed with opaque `BadRequest` errors.

### Resolution

The pagination design was simplified by replacing the `Until` loop with:

```text
ForEach + @range()
```

The current page number was obtained using:

```text
item()
```

For example, the January pagination range could be generated dynamically from the calculated number of pages.

### Result

The simplified `ForEach` implementation successfully processed the pagination test.

### Key Learning

Simpler orchestration patterns are generally easier to debug, maintain, and explain.

When two approaches provide the same functionality, the less complex design is usually preferable.

---

## 4. Concurrent Page Requests Caused API Timeouts

### Problem

The first `ForEach` pagination test attempted to retrieve several API pages concurrently.

Some Copy activities completed quickly while others timed out.

### Root Cause

Multiple large requests were being sent to the public Chicago API simultaneously.

The source API did not consistently handle the concurrent workload.

### Resolution

The page-level `ForEach` activity was changed to sequential execution.

The HTTP request timeout was also increased to allow slower requests more time to complete.

### Result

The multi-page pagination test completed successfully after the page-level requests were executed sequentially.

### Key Learning

Higher concurrency does not automatically improve ingestion performance.

Parallelism must be controlled according to the capabilities and limitations of the source system.

---

## 5. Deep Monthly Pagination Became Extremely Slow

### Problem

The original historical ingestion strategy processed one complete month at a time.

For example:

```text
January 2025
Rows: 7,607,290
Pages: 153
```

```text
February 2025
Rows: 7,288,575
Pages: 146
```

As page numbers increased, API response times became significantly slower.

Some early pages completed quickly, while later pages could take tens of minutes or eventually time out.

During one February run, only around 51 pages completed after more than 24 hours.

### Root Cause

The source API was performing deep pagination across multi-million-row monthly result sets.

Later pages became increasingly expensive for the API to resolve.

### Resolution

The ingestion architecture was redesigned from **monthly windows** to **daily date windows**.

Instead of:

```text
February
→ 7M+ rows
→ pages 1–146
```

the pipeline now processes:

```text
2025-02-01
→ get daily row count
→ calculate required pages
→ shallow pagination

2025-02-02
→ get daily row count
→ calculate required pages
→ shallow pagination
```

Once the daily windows became independent, controlled parent-level concurrency was enabled:

```text
Sequential = Off
Batch count = 4
```

This allowed up to four daily windows to be processed concurrently.

### Key Learning

The most effective optimization was not simply increasing parallelism.

The source query itself had to become smaller and cheaper.

The improved strategy became:

```text
Filter first
→ reduce result-set size
→ shallow pagination
→ controlled parallelism
```

---

## 6. Web Activity Returned HTTP `408 Request Timeout`

### Problem

The `Get_Window_Row_Count` Web activity occasionally failed with:

```text
HTTP 408 RequestTimeout
```

Some source API count queries required longer than the default ADF Web activity response timeout.

### Resolution

The Web activity was configured with:

```text
Request timeout: 00:10:00
Retry attempts: 2
Retry interval: 120 seconds
```

### Result

Most daily row-count requests subsequently completed successfully.

Temporary source-side delays could also be recovered automatically through retries.

### Key Learning

Timeout and retry policies should be configured explicitly when working with external APIs.

Transient source failures should not require manual intervention when they can be recovered safely through retries.

---

## 7. Historical Pipeline Was Not Restart-Safe

### Problem

The initial historical pipeline attempted to process every API page again whenever the pipeline was restarted.

For a long-running backfill, this could cause already successful pages to be downloaded unnecessarily.

### Resolution

A restart-safe pattern was introduced inside the page-level loop:

```text
ForEach Page
      ↓
Get Metadata
      ↓
Does expected file exist?
      ↓
   Yes / No
    ↓     ↓
   Skip  Copy
```

Each output file uses a deterministic filename containing the date and page number.

Example:

```text
chicago_rideshare_2025_02_01_page_1.parquet
```

### Result

The ingestion became idempotent at the file level.

When the pipeline is rerun:

- existing files are skipped
- missing files are processed
- previously completed work is preserved

### Key Learning

Restartability should be designed into long-running ingestion pipelines from the beginning.

A failed backfill should not require successfully processed data to be downloaded again.

---

## 8. Child Pipeline Failures During Historical Backfill

### Problem

During the full historical backfill, a small number of daily child pipelines failed because of temporary API timeouts.

One identified failed window was:

```text
2025-04-26T00:00:00
to
2025-04-27T00:00:00
```

The `Get_Window_Row_Count` activity exhausted its configured timeout and retry attempts.

### Resolution

The active parent pipeline was allowed to continue processing the remaining daily windows.

Failed child windows were recovered afterward through reruns.

Because deterministic filenames and file-existence checks were already implemented, previously completed files were automatically skipped.

### Key Learning

A large historical backfill should not require a complete restart because of a small number of transient source failures.

Idempotent processing allows failed subsets to be recovered independently.

---

## 9. Parent Pipeline Still Showed `Failed` After Child Recovery

### Problem

Some failed child pipelines were rerun manually and completed successfully.

However, the original parent pipeline continued to display:

```text
Failed
```

### Root Cause

ADF preserves the execution status of the original parent run.

A separately rerun child pipeline creates a new successful execution but does not retroactively change the status of the original parent execution.

### Resolution

Final ingestion success was determined through:

- data completeness
- date coverage
- monthly row counts
- source-to-Bronze reconciliation
- PySpark validation

rather than relying only on the visual status of the historical parent run.

### Key Learning

Pipeline execution status and final dataset correctness are related but not identical.

The actual data must always be validated independently.

---

## 10. Databricks Could Not Access the New `lakehouse` Container

### Problem

The project originally used separate ADLS containers:

```text
bronze
silver
gold
```

The storage architecture was later consolidated into:

```text
lakehouse/
├── bronze/
├── silver/
└── gold/
```

When attempting to access the new `lakehouse` container from Databricks, the operation failed with:

```text
SparkKeyProviderException
Invalid configuration value detected for fs.azure.account.key
```

### Root Cause

The new container had not yet been configured as a Unity Catalog external location using the existing Azure managed identity.

### Resolution

A Unity Catalog external location was created for:

```text
abfss://lakehouse@chicagoridedl.dfs.core.windows.net/
```

using the Azure Databricks Access Connector and managed-identity storage credential.

### Validation

The January dataset was migrated and validated again:

```text
Expected files: 153
Actual files:   153
```

```text
Expected rows: 7,607,290
Actual rows:   7,607,290
```

### Key Learning

Azure RBAC and Databricks Unity Catalog authorization are separate layers of storage access control.

Successful Azure-level permissions do not automatically guarantee Databricks access to a new storage path.

---

## 11. Copy Activity Retained a Hard-Coded January Filter

### Problem

After the first February–December historical backfill completed, PySpark validation returned:

```text
Total rows: 102,557,290
month_number = 1
```

All records appeared to belong to January even though the files had been written into February through December folders.

### Root Cause

The pipeline had only been partially parameterized.

The following components correctly used dynamic values:

- row-count Web activity
- output folders
- output filenames
- daily date windows
- parent orchestration

However, the actual Copy activity request body still contained the original January date range:

```text
2025-01-01T00:00:00
to
2025-02-01T00:00:00
```

ADF therefore calculated the correct number of pages for later dates while repeatedly downloading January records.

### Detection

The issue was not detected through ADF activity status because the orchestration itself was largely successful.

It was discovered during PySpark Bronze validation when every record produced:

```text
month_number = 1
```

### Resolution

The Copy activity request body was changed to use:

```text
pipeline().parameters.p_start_date
```

and:

```text
pipeline().parameters.p_end_date
```

The corrected Copy activity therefore generated daily source windows such as:

```text
2025-02-02T00:00:00
to
2025-02-03T00:00:00
```

### Smoke Test

Before rerunning the complete historical ingestion, a two-day February smoke test was performed.

The validation returned:

```text
Minimum timestamp: 2025-02-01 00:00:00
Maximum timestamp: 2025-02-02 23:45:00
Distinct dates: 2025-02-01, 2025-02-02
Month: 2
```

Only after the smoke test passed was the full February–December backfill rerun.

### Key Learning

Parameterizing destination folders and filenames does not guarantee that the source query itself is parameterized.

Most importantly:

> **A successful pipeline execution does not automatically mean the data is correct.**

---

## 12. Final Bronze Validation

After correcting the Copy activity and rerunning the historical ingestion, the complete 2025 Bronze dataset was validated using PySpark in Azure Databricks.

### Final Dataset Size

```text
Total rows:    93,514,416
Total columns: 21
```

### Coverage Validation

```text
Months represented:          12 / 12
Calendar dates represented: 365 / 365
Minimum timestamp:          2025-01-01 00:00:00
Maximum timestamp:          2025-12-31 23:45:00
Duplicate trip IDs:         0
```

### Monthly Row Counts

| Month | Rows |
|---|---:|
| January | 7,607,290 |
| February | 7,288,575 |
| March | 8,037,030 |
| April | 7,535,395 |
| May | 8,087,664 |
| June | 7,690,883 |
| July | 8,039,804 |
| August | 8,219,082 |
| September | 7,496,069 |
| October | 8,157,275 |
| November | 7,627,660 |
| December | 7,727,689 |
| **Total** | **93,514,416** |

January was also revalidated against the previously confirmed source count:

```text
Original January rows: 7,607,290
Final January rows:    7,607,290
```

February matched the API source count:

```text
Source rows: 7,288,575
Bronze rows: 7,288,575
```

### Key Learning

Validation became a required stage of the architecture rather than an optional final check.

File existence and successful pipeline execution alone are not enough to prove data completeness or correctness.

---

## 13. Bronze Data Quality Findings

Before designing the Silver transformation layer, the complete Bronze dataset was profiled to identify actual data-quality issues.

### Critical Null Checks

| Field | Null Rows |
|---|---:|
| `trip_id` | 0 |
| `trip_start_timestamp` | 0 |
| `trip_end_timestamp` | 0 |
| `trip_seconds` | 4,725 |
| `trip_miles` | 0 |

### Business Field Nulls

| Field | Null Rows |
|---|---:|
| `fare` | 409,142 |
| `tip` | 409,142 |
| `additional_charges` | 409,142 |
| `trip_total` | 409,142 |
| `pickup_community_area` | 16,577,851 |
| `dropoff_community_area` | 17,740,003 |
| `shared_trip_authorized` | 0 |
| `trips_pooled` | 0 |

Missing community-area values will be retained rather than dropping a significant proportion of otherwise usable trips.

These records can still contribute to non-geographic analysis while geographic reporting handles missing locations explicitly.

### Invalid Numeric and Timestamp Checks

The following conditions were evaluated:

```text
trip_seconds <= 0
trip_miles < 0
fare < 0
trip_total < 0
trip_end_timestamp < trip_start_timestamp
```

Results:

| Condition | Rows |
|---|---:|
| Non-positive `trip_seconds` | 354 |
| Negative `trip_miles` | 0 |
| Negative `fare` | 0 |
| Negative `trip_total` | 0 |
| End timestamp before start timestamp | 4,476 |

No negative mileage, fare, or trip-total values were identified.

### Key Learning

Data-quality decisions should be based on actual profiling results.

Large groups of otherwise usable records should not be removed simply because one analytical attribute is missing.

---

## 14. Duration Quality Investigation

### Problem

Bronze profiling identified records with missing or invalid trip durations.

Initial findings were:

```text
Null trip_seconds:           4,725
Non-positive trip_seconds:     354
End-before-start rows:       4,476
```

A deeper investigation was required to determine which records could be safely repaired and which should be quarantined.

### Investigation

All 4,476 records where:

```text
trip_end_timestamp < trip_start_timestamp
```

also contained:

```text
trip_seconds = NULL
```

These records could not be reliably repaired because the timestamps themselves were inconsistent.

Among records with valid timestamp ordering but missing or non-positive duration, 603 records required further investigation.

The breakdown was:

| Condition | Rows |
|---|---:|
| Null `trip_seconds` with positive timestamp-derived duration | 79 |
| Non-positive `trip_seconds` with positive derived duration | 0 |
| Null `trip_seconds` with zero derived duration | 170 |
| Non-positive `trip_seconds` with zero derived duration | 354 |

### Silver-Layer Decision

#### Recoverable Records

```text
79 rows
```

These records have missing `trip_seconds`, but the start and end timestamps produce a valid positive duration.

The Silver layer will:

```text
derive trip_seconds from timestamps
→ retain the record
→ set an imputation flag
```

#### Unrecoverable Records

The remaining invalid records consist of:

```text
4,476 reversed-timestamp records
+
170 null-duration / zero-derived-duration records
+
354 non-positive-duration / zero-derived-duration records
=
5,000 records
```

These records cannot be repaired reliably.

They will be separated into a quarantine or invalid-record dataset instead of being silently dropped or artificially corrected.

### Key Learning

Data-quality rules should distinguish between **recoverable** and **unrecoverable** records.

A transformation pipeline should not automatically correct data when there is insufficient evidence to determine the correct value.

---

# Overall Engineering Takeaway

The Bronze ingestion architecture evolved through several iterations as issues were identified, diagnosed, and resolved:

```text
Simple REST ingestion
        ↓
Parameterized API requests
        ↓
Pagination
        ↓
Restart-safe file checks
        ↓
Daily date-window ingestion
        ↓
Controlled concurrency
        ↓
Source-to-Bronze reconciliation
        ↓
Data-quality profiling
```

The most important lesson from the Bronze phase was:

> **A pipeline can complete successfully while still producing incorrect data.**

The hard-coded January source filter demonstrated that successful orchestration, correctly named files, and valid storage paths are not sufficient evidence that the underlying data is correct.

For that reason, the final ingestion design prioritizes:

- parameterized source queries
- deterministic file naming
- idempotent ingestion
- restart-safe processing
- controlled API concurrency
- timeout and retry handling
- source-to-target reconciliation
- date and row-count validation
- duplicate detection
- PySpark-based data-quality profiling
- explicit handling of recoverable and unrecoverable records
- reproducibility

The troubleshooting process also reinforced an important engineering principle:

> **Validate the data itself, not just the pipeline that moved it.**

This principle will continue into the Silver and Gold layers, where transformations and business metrics will be validated against clearly defined data-quality rules.
