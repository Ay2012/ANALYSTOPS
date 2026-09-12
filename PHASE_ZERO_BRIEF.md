# Phase Zero Brief

## Aim

Phase Zero establishes the business and engineering contract for AnalystOps before implementation begins. The goal is to turn the project blueprint into a concrete, testable product direction: who the platform serves, what operational spreadsheet problem it solves, what data contract it enforces, and what Phase One must prove.

AnalystOps should be a reliable DataOps platform enhanced by AI, not an LLM wrapper. AI should reason about ambiguity. Deterministic systems should execute business-critical changes. Every important output should be validated before it is trusted.

## Current Workspace

The workspace currently contains source material, not an application codebase.

- `AnalystOps_Project_Blueprint.pdf`: project blueprint describing product vision, architecture, phases, risks, and expectations.
- `data/online_retail_II.xlsx`: UCI Online Retail II workbook with two sheets and 1,067,371 transaction rows.
- `data/online+retail+ii.zip`: zipped copy of the source workbook.

Observed workbook shape:

- Sheets: `Year 2009-2010`, `Year 2010-2011`.
- Columns: `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer ID`, `Country`.
- Date range: December 1, 2009 through December 9, 2011.
- Data quality signals: missing customer IDs, missing descriptions, negative quantities, cancellation invoices, non-positive prices, country skew toward the United Kingdom.

These quirks are useful. They give the simulator realistic raw material for returns, incomplete customer data, malformed source submissions, and row-count distribution checks.

## Phase Zero Scope

Phase Zero should answer ten questions:

1. What fictional company is this platform serving?
2. Who produces the Excel files?
3. Who consumes the dashboards and reports?
4. Which business decisions depend on this data?
5. What is the canonical transaction schema?
6. What are the first business KPIs?
7. What file-arrival expectations define normal operations?
8. What initial data-quality contract should files satisfy?
9. What platform SLOs define acceptable service behavior?
10. What exact criteria make Phase One complete?

## Proposed Execution

1. Use the blueprint and source workbook profile as the factual baseline.
2. Write the first product requirements document at `docs/product_requirements.md`.
3. Keep Phase Zero document-only. No ingestion app, warehouse, dashboard, agents, or simulator code yet.
4. Challenge assumptions that could cause architecture problems later.
5. Move to Phase One only after the PRD has stable acceptance criteria.

## Working Product Frame

AnalystOps serves a medium-sized multi-country retail and e-commerce company. Regional operations teams export recurring Excel transaction reports from local systems. Central analytics, finance, merchandising, and operations teams depend on those reports for revenue, customer, product, returns, and freshness monitoring.

The system must reduce manual analyst cleanup while preserving traceability and operational control. It should tolerate schema drift, messy spreadsheets, late files, duplicate uploads, incomplete exports, and ambiguous field names without letting bad data corrupt downstream analytics.

## Non-Goals

Phase Zero will not:

- Generate the full application scaffold.
- Add LLM or agent code.
- Build dashboards.
- Introduce dbt, PostgreSQL, orchestration, or Kubernetes.
- Clean or modify the original source workbook.
- Optimize for technology count instead of measurable reliability.

## Key Assumptions To Challenge

- The source dataset has no explicit store or department field, so store/business-unit behavior must be simulated from country, month, or synthetic assignments.
- Customer ID is missing for a large share of rows, so it should not be a universal hard requirement.
- Negative quantities and cancellation invoices appear to represent returns or cancellations, so the data-quality contract must distinguish valid business events from invalid values.
- Country/month splitting may create very small files for low-volume countries, so row-count anomaly rules need country-aware baselines.
- Phase One should remain deterministic. AI begins later, after the simulator, manifests, and baseline validation are trustworthy.

## Phase One Readiness Criteria

Phase One can begin when `docs/product_requirements.md` defines:

- The fictional organization and operational workflow.
- Producers, consumers, and decisions supported by the data.
- Canonical transaction schema.
- First 5-8 business KPIs.
- File-arrival expectations.
- Initial data-quality contract.
- Initial platform SLOs.
- Exact Phase One success criteria.

