# AnalystOps Product Requirements

## 1. Product Objective

AnalystOps automates the safe intake of messy recurring operational spreadsheets for a retail and e-commerce company. It should convert heterogeneous Excel submissions into validated analytics-ready data while preserving lineage, auditability, and human control over ambiguous cases.

The core product promise is trusted automation. AnalystOps should reduce manual analyst effort without allowing AI or malformed spreadsheets to directly corrupt production data.

## 2. Fictional Business Scenario

The fictional company is Meridian Retail Group, a medium-sized retailer with e-commerce and regional sales operations across multiple countries. Each country team exports recurring transaction reports from local point-of-sale, e-commerce, or finance systems. The reports share a broad transaction concept but differ in worksheet layout, field names, formatting, completeness, and timing.

Central analytics receives these files, validates them, and prepares business dashboards for executives, finance, merchandising, operations, and data-quality owners.

## 3. Source Data Baseline

Phase Zero uses the UCI Online Retail II workbook as ground truth.

- Source file: `data/online_retail_II.xlsx`.
- Sheets: `Year 2009-2010`, `Year 2010-2011`.
- Total rows: 1,067,371 transaction lines.
- Source columns: `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer ID`, `Country`.
- Date range: December 1, 2009 through December 9, 2011.
- Known data traits: missing customer IDs, missing descriptions, returns/cancellations, non-positive prices, duplicate-risk scenarios, and strong country-volume skew.

The original workbook must remain untouched. Simulated submissions should be generated from it with reproducible manifests.

## 4. Upstream Producers

Initial file producers:

- Country operations teams.
- Regional finance analysts.
- E-commerce operations analysts.
- Store operations coordinators, simulated where the source data lacks explicit store identifiers.

Each producer submits a recurring Excel workbook for a country/month or business-unit/month reporting period.

## 5. Downstream Consumers

Initial consumers:

- Executive leadership, for revenue and growth monitoring.
- Finance, for monthly revenue, returns, and reconciliation checks.
- Merchandising, for product performance and demand patterns.
- Operations, for file freshness, late submissions, and rejected-file visibility.
- Data and analytics owners, for schema drift, validation failures, and lineage.

## 6. Business Decisions Supported

AnalystOps should support decisions about:

- Whether reported sales are ready for dashboard publication.
- Which countries or periods need analyst review before close.
- Which products, countries, or customer segments are driving revenue movement.
- Whether revenue changes are business events or data-quality issues.
- Where operational teams need follow-up for late, duplicate, incomplete, or malformed files.
- Whether automation is safe enough to reduce manual spreadsheet cleanup.

## 7. Canonical Transaction Schema

The initial canonical transaction schema is:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `invoice_id` | string | yes | Source invoice identifier. Cancellation invoices remain traceable. |
| `product_id` | string | yes | Source stock code or equivalent product key. |
| `product_description` | string | no | Nullable because the source contains missing descriptions. |
| `quantity` | integer | yes | Negative values are allowed only when classified as return or cancellation lines. |
| `transaction_timestamp` | datetime | yes | Parsed from the source transaction date. |
| `unit_price` | decimal | yes | Must be numeric after normalization. Non-positive values require review unless explicitly classified. |
| `customer_id` | string | no | Nullable because many source rows lack customer IDs. |
| `country` | string | yes | Required for country-level reporting and file partition checks. |
| `source_file_id` | string | yes | Added during ingestion for lineage. |
| `source_row_number` | integer | yes | Added during ingestion for row-level traceability. |

Derived fields such as `line_revenue`, `order_month`, `is_return`, and `is_cancellation` may be created later in deterministic build or warehouse layers.

## 8. Initial Business KPIs

The first business KPI set should be small and traceable:

1. Net revenue.
2. Gross sales before returns.
3. Order count.
4. Average order value.
5. Active customers.
6. Units sold and units returned.
7. Return or cancellation rate.
8. Revenue by country and product.

Platform-health metrics are separate from business KPIs and should include files received, files processed, files quarantined, validation failures, freshness, and manual-review rate.

## 9. File-Arrival Expectations

Initial operating assumption:

- Each country or business unit submits one workbook per reporting month.
- Files are expected by 09:00 local time on the second business day after month-end.
- Re-uploads are allowed but must be detected by file hash and processing version.
- Duplicate prior-period uploads should not create duplicate warehouse records.
- Late, missing, incomplete, or unexpectedly small files should be visible as operational issues.

Phase One should simulate this cadence from the historical source workbook.

## 10. Initial Data-Quality Contract

A submitted file is processable when:

- It contains a recognizable transaction table.
- It provides mappable equivalents for invoice, product, quantity, timestamp, unit price, and country.
- Required fields are present after schema mapping.
- Dates are parseable and fall inside the submitted reporting period, allowing a documented tolerance for boundary cases.
- Quantity and unit price can be parsed to numeric types.
- Exact duplicate rows are either removed by an approved deterministic operation or flagged.
- Row counts, null rates, revenue, customer counts, product counts, and return rates are within expected thresholds for that country/month.

A file should be quarantined or sent for review when:

- Required fields are missing or unmappable.
- The workbook is corrupt or lacks a transaction sheet.
- Parsing failures exceed an accepted threshold.
- Row count drops unexpectedly, such as a file containing only 10% of expected rows.
- The file appears to be a duplicate upload.
- Spreadsheet content appears to contain prompt-injection text or unsafe formulas.
- A transformation plan would require an operation outside the approved registry.

## 11. Initial Platform SLOs

These are starting targets and should be calibrated as the system matures:

- 100% of accepted rows retain source file and source row lineage.
- 100% of duplicate file hashes are detected before ingestion.
- Known-schema files continue through deterministic processing when the LLM provider is unavailable.
- Unknown-schema files queue for review when the LLM provider is unavailable.
- p95 time from file receipt to profiling result is under 2 minutes for a standard monthly file.
- p95 time from valid file receipt to dashboard-ready data is under 30 minutes after the warehouse and dashboard phases exist.
- False auto-approval rate is near zero and must be measured with ground-truth failure cases before autonomy thresholds are trusted.

## 12. Phase One Success Criteria

Phase One is complete when the project has a deterministic dataset and failure simulator with no AI dependency.

Required outcomes:

- The original UCI workbook remains unchanged.
- A loader reads both yearly sheets and normalizes source records into a consistent internal representation.
- A splitter generates recurring country/month or business-unit/month Excel submissions.
- A reproducible corruption injector supports seeded failure scenarios.
- Each generated file has a ground-truth manifest describing source rows, expected schema, injected failures, and expected handling.
- The simulator can create at least these scenarios: renamed columns, currency strings, date-format changes, duplicate rows, missing customer IDs, missing required columns, unexpected columns, incomplete files, duplicate prior-month uploads, and multi-sheet workbooks.
- Automated tests prove reproducibility for a fixed seed.
- Automated tests prove that generated manifests match the files produced.
- The source workbook is treated as read-only input.

Non-goals for Phase One:

- No schema-understanding agent.
- No transformation executor beyond simulator support code.
- No warehouse, dbt project, dashboard, anomaly detection, or management summary.
- No Kubernetes.

## 13. Architecture Risks To Resolve Early

- Source data does not contain explicit store or department fields. Simulated business units must be generated transparently rather than implied as real source facts.
- Customer ID is frequently missing, so customer-level KPIs must distinguish known-customer metrics from total transaction metrics.
- Returns and cancellations must be modeled as business events, not blanket validation failures.
- Small-country files may naturally have low row counts, so row-count anomaly detection needs country/month baselines.
- The approved operation registry should exist before the schema agent so the agent cannot invent executable behavior.
- PRD terms such as auto-approved, processable, quarantined, and manual review need stable definitions before metrics are reported.

