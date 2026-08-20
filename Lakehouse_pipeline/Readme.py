# Lakehouse Transaction Pipeline (Databricks Free Edition)

A small end-to-end Medallion pipeline (Bronze → Silver → Gold) for client-supplied
customer transaction files (CSV + JSON), built with Auto Loader, Delta Lake, and
PySpark/Spark SQL.

## Files

| File | Purpose |
|---|---|
| `00_setup_and_sample_data.py` | Creates the catalog/schema/volumes and generates sample CSV/JSON transaction files (with intentional duplicates, nulls, and a schema change) into a "landing" volume |
| `01_bronze_ingestion.py` | Auto Loader ingestion into `bronze_transactions` |
| `02_silver_transformation.py` | Cleansing, validation, dedup, business logic → `silver_transactions` (+ `silver_quarantine`) |
| `03_gold_aggregation.py` | Incremental aggregation via Change Data Feed → three Gold reporting tables |
| `04_monitoring_and_maintenance.py` | Reconciliation, DQ trend, freshness, and table maintenance checks |

Import these as notebooks into a Databricks Free Edition workspace and run in order
(00 → 01 → 02 → 03). Run 00 again (or the `generate_incremental_drop()` helper) to drop a
new file and re-run 01–03 to see incremental processing pick up only the new data.

## 1. Architecture & approach

```
landing volume (CSV + JSON files, arriving on a schedule)
        │  Auto Loader (cloudFiles), one stream per format
        ▼
BRONZE  bronze_transactions          — raw, append-only, schema-evolving, full lineage
        │  Structured Streaming (readStream.table) + foreachBatch
        ▼
SILVER  silver_transactions          — typed, deduplicated, validated, business logic applied
        silver_quarantine            — rejected rows + reasons, for triage
        │  Change Data Feed + foreachBatch MERGE
        ▼
GOLD    gold_revenue_by_customer_daily
        gold_revenue_by_month
        gold_category_performance
```

Each layer is a **separate, independently re-runnable streaming job** using
`trigger(availableNow=True)`. This gives the operational simplicity of a batch job
(runs, processes what's new, stops) while reusing Structured Streaming's built-in exactly-once
file/offset tracking and checkpointing — no hand-rolled "have I seen this file before" logic.

- **Bronze** ingests CSV and JSON separately (Auto Loader needs one format per stream) but
  writes into a single unified Bronze table, with the raw source format and file path
  preserved. Nothing is cleaned, cast, or dropped here — Bronze is the replayable source of
  truth if a Silver bug is ever found and history needs reprocessing.
- **Silver** deduplicates and validates in a single `foreachBatch`, using a `MERGE` keyed on
  `transaction_id` for the good rows, and appends the bad rows (with reasons) to a
  quarantine table rather than discarding them silently.
- **Gold** consumes Silver's **Change Data Feed** instead of rescanning the whole table, and
  recomputes only the affected reporting keys (day/month/category) before `MERGE`-ing them
  in — this stays cheap as history grows and correctly handles late-arriving corrections to
  older transactions.

## 2. Key technical decisions

- **Auto Loader over plain batch reads**: gives incremental file discovery, exactly-once
  processing via checkpoints, and native schema inference/evolution — the client can add a
  new column (`discount_pct` arrives in day 3's files in the sample data) without breaking
  the pipeline.
- **`addNewColumns` schema evolution + `rescuedDataColumn`**: new expected columns are
  absorbed automatically; anything genuinely unexpected is captured in `_rescued_data`
  rather than causing a stream failure or silent data loss.
- **`MERGE` instead of `append` in Silver and Gold**: this is what makes the pipeline
  **idempotent**. Re-running a job, reprocessing an old Bronze batch, or a source system
  re-sending the same file all converge on the same end state rather than creating
  duplicates. Deduplication also happens *within* a micro-batch (row_number over
  `transaction_id`) before the merge, since a single file can itself contain repeats.
  Note that the sample uses the transaction's business key (`transaction_id`) as the merge
  key rather than a hash-of-row, so it is genuinely idempotent even if a row's non-key
  values changed between runs (e.g. a status correction) — it updates rather than duplicates.
- **Reject, don't drop**: invalid records (null keys, bad currency codes, non-positive
  amounts on non-refunds, etc.) go to `silver_quarantine` with structured reasons. This
  keeps Silver trustworthy for reporting while still preserving every record for the client
  to investigate — nothing to reprocess is lost.
- **Gold via CDF rather than full recompute**: with a client sending regular incremental
  files, recomputing three full aggregate tables from scratch on every run doesn't scale.
  Reading only changed rows and refreshing only the affected keys keeps Gold's cost
  proportional to what changed, not to total history size.
- **Partitioning**: Silver is partitioned by `transaction_month` — a reasonable cardinality
  for this volume and a natural filter for most reporting/backfill queries. `OPTIMIZE`
  (Z-ORDER on `customer_id` for Silver) is used for small-file compaction and read
  performance, run as routine maintenance rather than after every micro-batch.
- **Unity Catalog volumes** for landing/checkpoint/schema storage, and three-level
  `catalog.schema.table` naming throughout, in line with how Databricks Free Edition (and
  any modern Databricks workspace) expects storage and governance to be organised.

## 3. How I'd productionise this

- **Orchestration**: a Databricks **Workflow (Job)** with one task per notebook
  (00 excluded — that's a demo-only step), Bronze → Silver → Gold as sequential task
  dependencies, on a schedule matching how often the client actually delivers files (e.g.
  hourly or daily). Use **Job clusters** (or serverless jobs compute), not an always-on
  interactive cluster, for cost control.
- **File arrival trigger**: rather than a fixed schedule, use Auto Loader **file
  notification mode** (cloud storage events, e.g. S3+SQS or ADLS+Event Grid) so Bronze
  ingestion starts as soon as files land, instead of polling.
- **Alerting**: Job-level email/Slack/webhook notifications on task failure; a Databricks
  SQL alert on the freshness and quarantine-rate queries in `04_monitoring_and_maintenance`
  (e.g. "alert if quarantine rate > 5% today" or "alert if Gold hasn't updated in >26h").
- **Data quality**: promote the ad-hoc checks in Silver to **Delta Live Tables expectations**
  (or Lakehouse Monitoring on the Silver/Gold tables) for built-in DQ metrics, dashboards,
  and quarantine handling with less custom code — I used foreachBatch here to keep the
  logic fully visible and portable for the exercise, but DLT is the more idiomatic
  production choice on Databricks.
- **Secrets/config**: source paths, catalog/schema names, and thresholds would move out of
  hard-coded notebook constants into **Job parameters** / a small config file, so the same
  notebooks serve dev/test/prod via widgets rather than being edited per environment.
- **Access control**: Unity Catalog grants scoped per layer (e.g. analysts get `SELECT` on
  Gold only; the ingestion service principal gets write access to Bronze/Silver/Gold and
  read-only on the landing volume).
- **CI/CD**: notebooks (or `.py` files, as here, which are already source-control friendly)
  in a Git repo, Databricks Asset Bundles (DAB) to deploy the Job definition and notebook
  code together, with separate dev/staging/prod bundle targets.
- **Testing**: unit tests for the transformation functions (`clean_and_standardise` etc. are
  written as plain functions taking/returning DataFrames specifically so they're testable
  with `chispa`/`pytest` outside of a live streaming context), plus a small set of
  known-good/known-bad sample records checked into the repo as fixtures.

## 4. What I'd do differently at large scale

- **Move Bronze/Silver DQ and orchestration to Delta Live Tables (Lakeflow Declarative
  Pipelines)**: at scale, DLT's built-in expectations, lineage graph, auto-scaling clusters,
  and unified batch/streaming semantics remove a lot of the hand-written
  checkpoint/foreachBatch plumbing used here for clarity.
- **Partitioning/liquid clustering**: for genuinely large data, I'd benchmark
  `transaction_month` partitioning against **Liquid Clustering** on `customer_id`/
  `transaction_date` — Liquid Clustering avoids the "too many small partitions" problem you
  get with high-cardinality or multi-dimensional access patterns as volume grows.
  I'd also reconsider partition granularity (e.g. daily instead of monthly) once file/row
  volume justifies it.
- **Schema drift governance**: `addNewColumns` is a reasonable default for a demo, but at
  scale I'd pair it with **schema change notifications** (Auto Loader can trigger a stream
  restart with an explicit error) or a light contract/registry step so unexpected structural
  changes are surfaced to a data engineer rather than silently absorbed.
- **Separate compute per layer with autoscaling**, and possibly separate landing zones /
  Auto Loader streams per client or per source system, so one noisy/large client's backlog
  doesn't starve or delay others sharing a job.
- **Backfill and reprocessing strategy**: for large historical reprocessing (e.g. a Silver
  bug fix), I'd use Bronze's `DESCRIBE HISTORY`/time-travel plus a dedicated backfill job
  with a widened trigger window, rather than replaying through the same incremental
  checkpoint used for day-to-day processing.
- **Cost/performance monitoring**: at scale I'd build a small ops dashboard on the
  `system.billing.usage` and job-run system tables (DBU cost per pipeline, per layer) so
  cost regressions are visible alongside data-quality regressions, not just as a monthly
  bill surprise.
- **Data contracts with the client**: formalise the expected schema/SLA (arrival frequency,
  required fields, allowed value ranges) with the client so quarantine/DQ rules are a shared
  agreement rather than something inferred purely from observed data.
