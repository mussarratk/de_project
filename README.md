# Databricks de_project
https://github.com/mussarratk/Databricks_Project
https://github.com/mussarratk/up4_End-to-End-Retail-Databricks-Project
https://github.com/mussarratk/data-e-203


## DP-750
---
# 🚕 Rideshare Lakeflow Declarative Medallion Pipeline

**A real-time + batch data pipeline built on Databricks Lakeflow Declarative Pipelines (LDP), implementing a full Bronze → Silver → OBT → Star Schema medallion architecture for rideshare trip data.**

> Built as a hands-on learning project to master Databricks' declarative pipeline framework — covering streaming ingestion, stateful watermarking, automated SCD (`AUTO CDC`), data quality enforcement, and multi-source append flows.

### 📑 Contents
[Project Summary](#-project-summary) · [Business Problem & Value](#-business-problem--value) · [Solution Approach](#-solution-approach) · [Architecture](#️-architecture) · [Tech Stack](#️-tech-stack) · [Repository Structure](#️-repository-structure) · [Pipeline DAG](#-pipeline-dag-as-built) · [What This Project Implements](#-what-this-project-implements) · [CI/CD — Azure DevOps](#-cicd--azure-devops) · [Databricks Jobs Orchestration](#️-databricks-jobs--end-to-end-orchestration) · [End-to-End Walkthrough](#-end-to-end-implementation-walkthrough-for-interview-qa) · [Business Outcomes](#-business-outcomes) · [Code Examples](#-code-examples) · [Key Concepts Learned](#-key-concepts-i-learned-building-this) · [Interview Talk Track](#-how-id-explain-this-project-in-an-interview) · [How to Run](#-how-to-run) · [Future Improvements](#-future-improvements)

---

## 📌 Project Summary

This project ingests rideshare trip events (rider app, driver app, and reference/batch data) and processes them through a governed medallion architecture entirely with **Lakeflow Declarative Pipelines** (Databricks' successor to Delta Live Tables), landing on a **star schema** (fact + dimension tables) ready for BI and analytics consumption.

| | |
|---|---|
| **Platform** | Databricks (Lakeflow Declarative Pipelines, Delta Lake, Unity Catalog) |
| **Languages** | Python (PySpark), SQL |
| **Pipeline pattern** | Medallion architecture — Bronze → Bronze (Mapped) → Silver → Gold OBT → Gold Star Schema |
| **Data domain** | Rideshare trips, customers, drivers, vehicles, payments |
| **Core techniques** | Auto Loader, Structured Streaming watermarks, stream-to-static joins, `AUTO CDC` (SCD Type 1/2), DLT expectations, append flows, pipeline parameters, event log monitoring |

---

## 💼 Business Problem & Value

**The problem:** A rideshare platform generates continuous, high-volume event streams (trip requests, driver assignments, fare completions) from multiple apps (rider, driver) plus reference data (customer profiles, vehicle registrations, payment methods) that changes independently over time. Without a governed pipeline, teams end up with:
- Stale or duplicated dashboards built on ad hoc, hand-maintained ETL scripts
- No reliable way to answer "what did this customer's profile look like *at the time* of a specific trip" (no change history)
- Silent data quality issues (nulls, bad fares, duplicate events) surfacing only after they've corrupted a report
- No shared visibility into whether a pipeline run succeeded, partially failed, or silently dropped data

**The business value this solution delivers:**
- **Trustworthy, analytics-ready data** for BI dashboards, ad-hoc analytics, and downstream ML/data science use cases — sourced from a single governed star schema instead of scattered ad hoc tables.
- **Accurate historical reporting** (e.g., "revenue by customer's home city at the time of the trip") via SCD Type 2 dimension history — not possible with a simple overwrite-based table.
- **Faster time-to-insight**: near real-time trip data flows from ingestion to Gold without waiting on a nightly batch window.
- **Reduced operational risk**: automated data quality gates and pipeline failure alerts catch issues before they reach a dashboard or stakeholder, instead of being discovered after the fact.
- **Lower engineering maintenance cost**: declarative pipeline definitions replace large amounts of hand-written, per-table boilerplate (see [code examples](#-code-examples) and the [legacy vs. Lakeflow comparison](./legacy_vs_lakeflow_dlt_code_comparison.md)), meaning less custom code to debug, review, and keep in sync as requirements change.

## 🎯 Solution Approach

The solution follows a **medallion architecture** so that raw, cleaned, and business-ready data are always clearly separated and independently reprocessable:

1. **Land everything raw, unmodified, in Bronze** — so the pipeline can always be replayed from source-of-truth data if downstream logic changes.
2. **Validate and conform in Silver** — apply schema mapping, watermarking (to safely handle late-arriving streaming events), and data quality expectations *before* anything reaches a business-facing table.
3. **Denormalize once into a Gold OBT** — join the enriched trip stream against current dimension snapshots, producing a single wide analytical table.
4. **Model the OBT into a star schema** — split into a `fact_trips` table (additive measures) and SCD Type 2 dimension tables (full history), which is the layer BI tools and analysts actually query.
5. **Automate everything that isn't business logic** — ingestion (Auto Loader), change tracking (`AUTO CDC`), quality gates (expectations), multi-source merging (append flow), and monitoring (event log + Runs UI) are all handled declaratively by the platform, so the codebase only expresses *what* the data should look like, not *how* to safely materialize it.

---

## 🏗️ Architecture

<img width="936" height="618" alt="image" src="https://github.com/user-attachments/assets/7a4f5896-138d-42e2-817d-c62c059d23d4" />

![Rideshare Lakeflow Declarative Medallion Architecture](docs/architecture-overview.png)
*(Add the architecture diagram image to a `docs/` folder in this repo and update the path above.)*

**Flow, end to end:**

```
SOURCES                                    CONSUMPTION
├── Rideshare App  ─┐                      ├── BI Dashboards
├── Driver App      ├─ Real-time (stream)  ├── Data Science / ML
├── Rider App       ┘                      ├── Ad-hoc Analytics
├── External APIs   ─┐                     └── APIs / Services
└── Reference Data   ┴─ Batch / CDC
         │
         ▼
┌─────────────┐   ┌──────────────────┐   ┌───────────────┐   ┌───────────────────┐   ┌──────────────────────┐
│   BRONZE    │──▶│  BRONZE (Mapped)  │──▶│    SILVER     │──▶│    OBT (GOLD)      │──▶│  GOLD — Star Schema  │
│ Raw Ingest  │   │ Schema Validation  │   │ Transform &   │   │ Stream-Static Join │   │ Facts + Dimensions   │
│ Auto Loader │   │   & Mapping        │   │  Enrichment   │   │  (append-only OBT) │   │  via AUTO CDC (SCD)  │
└─────────────┘   └──────────────────┘   └───────────────┘   └───────────────────┘   └──────────────────────┘
   (Delta Lake)        (Delta Lake)          (Delta Lake)          (Delta Lake)              (Delta Lake)
```

**Cross-cutting platform capabilities** (applied throughout every layer):
- **Data Quality Checks** — schema, null, range, and uniqueness checks via `@dlt.expect*`
- **Data Observability** — pipeline metrics, data drift, freshness, and volume tracking
- **Lineage & Catalog** — end-to-end lineage and discovery via Unity Catalog
- **Alerts & Notifications** — data quality alerts, pipeline failure notifications, latency alerts
- **Orchestration** — Lakeflow Declarative Pipelines, both continuous and triggered
- **Security & Governance** — RBAC, row-level security, audit logs, data masking

**Lakehouse foundation:** Databricks Lakehouse Platform · Delta Lake · Unity Catalog · Photon Engine · Auto Optimize
<img width="933" height="626" alt="image" src="https://github.com/user-attachments/assets/97abcf6d-c5bd-4fbc-9531-93b120f63c20" />

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Compute / Runtime** | Databricks (Lakeflow Declarative Pipelines runtime), Apache Spark (Structured Streaming), Photon Engine |
| **Storage format** | Delta Lake |
| **Cataloging / Governance** | Unity Catalog (three-level namespace, lineage, RBAC, row-level security, audit logs, data masking) |
| **Ingestion** | Auto Loader (`cloudFiles`), Kafka-compatible Azure Event Hub for streaming sources |
| **Languages** | Python (PySpark), SQL |
| **Pipeline framework** | Lakeflow Declarative Pipelines (`dlt` module) — `@dlt.table`, `@dlt.view`, `dlt.create_streaming_table`, `dlt.create_auto_cdc_flow`, `@dlt.append_flow`, `dlt.create_sink`, `@dlt.expect*` |
| **Orchestration** | Databricks Jobs (multi-task jobs, pipeline task type), pipeline-native triggered/continuous execution |
| **CI/CD** | Azure DevOps (Repos + Pipelines), Databricks Asset Bundles (DABs), Databricks CLI |
| **Monitoring** | Pipeline Runs UI, persisted pipeline event log (Delta table), Databricks Jobs alerts, failure notifications |
| **Performance/optimization** | Auto Optimize, (planned) Z-ordering / liquid clustering on dimension merge keys |

---

## 🗂️ Repository Structure

```
lpd_medallion/
├── explorations/
│   └── exploration.py          # ad-hoc SQL/PySpark exploration & validation queries
├── transformations/
│   ├── bronze_trips.py         # Bronze: Auto Loader ingestion of raw trip events
│   ├── silver_trips.py         # Silver: cleaning, watermarking, enrichment
│   ├── gold_obt.py             # Gold: stream-to-static join → One Big Table (append-only)
│   ├── stg_dimensions.py       # Staging views for each dimension (stg_dim_customers,
│   │                           #   stg_dim_drivers, stg_dim_vehicles, stg_dim_payments)
│   ├── dimensions.py           # dim_customers, dim_drivers, dim_vehicles, dim_payments
│   │                           #   via dlt.create_auto_cdc_flow (SCD Type 2)
│   └── fact.py                 # fact_trips — append-only fact table
├── docs/
│   ├── architecture-overview.png
│   ├── pipeline-dag-full.png
│   └── pipeline-run-detail.png
└── README.md
```

> Catalog path used in this project: `azuredatabricks_catalog.ldp_medallion` (Unity Catalog three-level namespace: catalog → schema → table).

---

## 🔀 Pipeline DAG (as built)

```
bronze_trips ──▶ silver_trips ──▶ gold_obt ──┬──▶ stg_dim_customers ──▶ dim_customers
                                              ├──▶ stg_dim_drivers   ──▶ dim_drivers
                                              ├──▶ stg_dim_vehicles  ──▶ dim_vehicles
                                              ├──▶ stg_dim_payments  ──▶ dim_payments
                                              └──▶ stg_dim_trips     ──▶ fact_trips
```

<img width="1303" height="634" alt="image" src="https://github.com/user-attachments/assets/f5484741-d324-4751-b2b9-0c0bb022680e" />

![Full pipeline DAG](docs/pipeline-dag-full.png)
![Pipeline run detail with upsert counts](docs/pipeline-run-detail.png)

A successful run shows each streaming dimension table reporting **"Upserted: N"** row counts (handled internally by `AUTO CDC`), plus expectation pass/fail counts surfaced directly on the graph node (e.g. `dim_customers` showing **1 expectation** flagged on a given run) — this is the built-in observability referenced above, with no custom logging table required.

---

## ✅ What This Project Implements

### 1. Bronze — Raw Ingestion
- **Auto Loader** (`cloudFiles`) incrementally ingests raw trip files from cloud storage.
- No manually maintained schema — schema inference and evolution handled automatically.
- Reference/mapping tables (drivers, vehicles, payments) ingested with lighter-weight handling than the primary trips stream.

### 2. Bronze (Mapped) → Silver — Validation, Mapping & Enrichment
- Schema validation and standardization.
- **Watermarking** (`withWatermark`) applied to the trips stream to bound state store growth and define how much event-time lateness is tolerated before late records are dropped from stateful processing.
- **Data quality expectations** (`@dlt.expect`, `@dlt.expect_all_or_drop`, `@dlt.expect_all_or_fail`) enforce null checks, range checks, and referential sanity — applied starting at this layer, not in raw Bronze.

### 3. Gold — OBT (One Big Table)
- **Stream-to-static join**: the watermarked trips stream is joined against static/snapshot dimension data in each micro-batch.
- Deliberately **append-only**, since it's built from a streaming source.

### 4. Gold — Star Schema (Facts & Dimensions)
- **`fact_trips`**: append-only, holding additive numeric measures (`distance_km`, `fare_amount`) plus foreign keys — grain defined as the combination of trip/customer/driver/vehicle/payment IDs.
- **Dimension tables** (`dim_customers`, `dim_drivers`, `dim_vehicles`, `dim_payments`): built via **`dlt.create_auto_cdc_flow`** with `stored_as_scd_type=2`, automatically preserving full change history (start/end-dated versioned rows) with zero hand-written `MERGE` logic.
- Each dimension is fed through a dedicated **staging view** (`stg_dim_*`) that isolates exactly which columns belong in the dimension before `AUTO CDC` processes them.

### 5. Automated Orchestration & Operations
This project deliberately pushes orchestration, scheduling, and monitoring into **platform configuration** rather than custom code:
- **Dependency resolution** — the DAG (`bronze_trips → silver_trips → gold_obt → stg_dim_* → dim_*/fact_trips`) is inferred automatically from each table's `spark.readStream`/`spark.read` source references — no manual task-dependency wiring like you'd write in Airflow/ADF.
- **Trigger modes** — supports both **triggered** (run once, process what's new, stop — good for scheduled batch-style runs) and **continuous/real-time** execution (`pipelines.trigger.realTime` for low-latency streaming) from the same codebase, without separate code paths.
- **Pipeline parameters** — environment-specific values (table names, connection strings) injected via pipeline configuration and read with `spark.conf.get(...)`, never hardcoded — enabling the same code to run unchanged across dev/staging/prod.
- **Scheduling** — pipeline attached as a Databricks Job task with CRON/interval scheduling, timezone control, and failure notifications configured once at the job level.
- **Monitoring** — built-in Runs tab (per-run status, duration, per-table record counts, upsert counts per `AUTO CDC` flow), persisted event log to a queryable Delta table for custom alerting, and "navigate to code" from any failed DAG node straight to the offending file.
- **Full refresh** — supports both full-pipeline and selective table refresh (`Select tables for refresh`) for safe reprocessing during development without disturbing unrelated tables.
- **Failure isolation** — each `append_flow` and each dimension's `AUTO CDC` flow runs independently, so a failure or expectation violation in one dimension doesn't take down unrelated tables in the same pipeline run (visible in the DAG screenshot above, where each `stg_dim_*` → `dim_*` pair reports its own status).

---

## 🔄 CI/CD — Azure DevOps

**What was done:** Code changes to the pipeline (`transformations/*.py`) are version-controlled in a Git repo linked to Databricks Repos, and promoted across environments through an **Azure DevOps YAML pipeline** using **Databricks Asset Bundles (DABs)** — instead of manually clicking through the Databricks UI to move code/config between dev, staging, and prod workspaces.

**Environments:** `dev` → `staging` → `prod`, each mapped to its own Databricks workspace / Unity Catalog catalog (e.g. `dev_catalog.ldp_medallion`, `prod_catalog.ldp_medallion`), so pipeline runs in one environment never touch another environment's data.

**Pipeline stages (`azure-pipelines.yml`):**
```yaml
trigger:
  branches:
    include: [main]
pr:
  branches:
    include: [main]

pool:
  vmImage: 'ubuntu-latest'

variables:
  - group: databricks-secrets   # DATABRICKS_HOST / DATABRICKS_TOKEN per environment, stored in Azure Key Vault-linked variable group

stages:

- stage: Validate
  jobs:
    - job: ValidateBundle
      steps:
        - script: pip install databricks-cli databricks-sdk
          displayName: 'Install Databricks CLI'
        - script: databricks bundle validate -t dev
          displayName: 'Validate bundle config (syntax, resource references)'
        - script: pytest tests/ --junitxml=test-results.xml
          displayName: 'Run unit tests on transformation logic'
        - task: PublishTestResults@2
          inputs:
            testResultsFiles: 'test-results.xml'

- stage: DeployDev
  dependsOn: Validate
  condition: succeeded()
  jobs:
    - deployment: DeployToDev
      environment: 'dev'
      strategy:
        runOnce:
          deploy:
            steps:
              - script: databricks bundle deploy -t dev
                displayName: 'Deploy pipeline + job definitions to Dev workspace'
              - script: databricks bundle run ldp_medallion_pipeline -t dev
                displayName: 'Trigger a pipeline run for smoke-testing in Dev'

- stage: DeployStaging
  dependsOn: DeployDev
  condition: succeeded()
  jobs:
    - deployment: DeployToStaging
      environment: 'staging'          # configured with a manual approval gate in Azure DevOps
      strategy:
        runOnce:
          deploy:
            steps:
              - script: databricks bundle deploy -t staging
                displayName: 'Deploy to Staging workspace'

- stage: DeployProd
  dependsOn: DeployStaging
  condition: succeeded()
  jobs:
    - deployment: DeployToProd
      environment: 'prod'             # manual approval gate + required reviewers
      strategy:
        runOnce:
          deploy:
            steps:
              - script: databricks bundle deploy -t prod
                displayName: 'Deploy to Production workspace'
```

**`databricks.yml` (Asset Bundle definition, environment-agnostic):**
```yaml
bundle:
  name: ldp_medallion

targets:
  dev:
    workspace:
      host: https://<dev-workspace>.azuredatabricks.net
    variables:
      catalog: dev_catalog
  staging:
    workspace:
      host: https://<staging-workspace>.azuredatabricks.net
    variables:
      catalog: staging_catalog
  prod:
    mode: production
    workspace:
      host: https://<prod-workspace>.azuredatabricks.net
    variables:
      catalog: prod_catalog

resources:
  pipelines:
    ldp_medallion_pipeline:
      name: ldp_medallion_pipeline
      catalog: ${var.catalog}
      target: ldp_medallion
      libraries:
        - notebook: { path: ./transformations/bronze_trips.py }
        - notebook: { path: ./transformations/silver_trips.py }
        - notebook: { path: ./transformations/gold_obt.py }
        - notebook: { path: ./transformations/stg_dimensions.py }
        - notebook: { path: ./transformations/dimensions.py }
        - notebook: { path: ./transformations/fact.py }
  jobs:
    ldp_medallion_job:
      name: ldp_medallion_job
      schedule:
        quartz_cron_expression: "0 0 * * * ?"   # hourly
        timezone_id: "UTC"
      tasks:
        - task_key: run_medallion_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.ldp_medallion_pipeline.id}
      email_notifications:
        on_failure: ["data-eng-team@company.com"]
```

**Benefits this delivers:**
- **Consistent, repeatable deployments** — the exact same bundle definition is applied to dev/staging/prod, eliminating "worked in dev, broke in prod" drift caused by manual UI configuration.
- **No manual click-ops** — pipeline definitions, job schedules, and cluster configs are all defined as code and reviewed via pull request, not clicked together by hand in each workspace.
- **Fast, safe rollback** — reverting a bad deploy is a Git revert + re-run of the pipeline, not a manual UI reconstruction.
- **Environment isolation with promotion gates** — staging/prod deploys require manual approval in Azure DevOps, preventing an untested change from reaching production data.
- **Full audit trail** — every deployment is tied to a Git commit, PR, and pipeline run ID, satisfying governance/compliance requirements.
- **Faster onboarding** — a new engineer can stand up an entire environment (`databricks bundle deploy -t dev`) from a fresh workspace in minutes instead of manually recreating pipeline/job configuration.

---

## ⚙️ Databricks Jobs — End-to-End Orchestration

While the Lakeflow Declarative Pipeline itself resolves *table-level* dependencies automatically, a **Databricks Job** wraps the pipeline to handle *pipeline-level* scheduling, sequencing with other work, and operational alerting — the layer an interviewer will expect you to speak to.

**How the job is structured (multi-task job):**
```
Task 1: run_medallion_pipeline        (Pipeline task — triggers the LDP pipeline itself)
   │
   ▼
Task 2: data_quality_summary_notebook (Notebook task — queries the persisted event log,
   │                                    posts a run summary to a Teams/Slack webhook)
   ▼
Task 3: refresh_downstream_dashboard  (SQL task — REFRESH on any dependent BI extract /
                                        materialized aggregate table, only runs if Task 1 succeeded)
```

- **Task dependency graph:** each downstream task declares `depends_on` the pipeline task, so the dashboard refresh never runs against a partially-updated Gold layer.
- **Trigger:** CRON schedule (hourly, matching business freshness requirements) defined in the bundle, with timezone pinned to avoid daylight-saving drift in reporting.
- **Compute:** the pipeline task uses **serverless/job-cluster compute** scoped to the job run (spun up on trigger, torn down on completion) rather than an always-on interactive cluster — controls cost since compute isn't paid for between runs.
- **Retries:** configured at the task level (e.g., 2 automatic retries with backoff) to absorb transient issues (a brief source outage, a cloud storage throttling blip) without paging anyone.
- **Concurrency control:** `max_concurrent_runs: 1` prevents overlapping runs if a run takes longer than the schedule interval, avoiding duplicate/overlapping writes to the same tables.
- **Alerting:** `email_notifications`/webhook alerts fire on `on_failure` and `on_duration_warning_threshold_exceeded`, routed to the data engineering team — paired with the pipeline's own event log for root-cause detail once alerted.
- **Parameterization:** the job passes environment-specific parameters (catalog name, source paths) down into the pipeline via the same `spark.conf.get(...)` pattern used elsewhere in the project, so the *same* job definition — deployed via the CI/CD bundle above — behaves correctly whether it's running in dev, staging, or prod.
- **Git-based job source:** the job is configured to pull its task source directly from the Git repo/branch tied to each bundle target, so a deploy is always running the code that's actually in version control — not a stale notebook copy someone forgot to sync.

**Benefits anticipated from this orchestration design:**
- **Single pane of glass for the full workflow** — the Databricks Jobs UI shows pipeline execution *and* downstream dashboard-refresh/notification steps as one coherent run, instead of disconnected pieces someone has to mentally stitch together.
- **Cost control** — job-scoped compute means you only pay for cluster time during actual processing, not for an idle always-on cluster.
- **Resilience without manual intervention** — retries absorb transient failures automatically; alerts only fire for issues that actually need a human.
- **Safe sequencing** — the dashboard/report layer only refreshes after the Gold layer is confirmed updated, preventing stakeholders from ever seeing a half-updated report.
- **Reproducible across environments** — because the job is deployed via the same Azure DevOps/DAB pipeline as the code, "how is this scheduled in prod vs. dev" has one clear, version-controlled answer.

---

## 🧭 End-to-End Implementation Walkthrough (for interview Q&A)

Use this as the mental model when a interviewer asks "walk me through what happens from a code change to that data landing in a dashboard":

1. **Developer** creates a branch in Databricks Repos (Git-backed), edits a transformation file (e.g. adds a new expectation rule to `silver_trips.py`), opens a PR against `main`.
2. **Azure DevOps CI** triggers automatically on the PR: validates the Asset Bundle (`databricks bundle validate`), runs unit tests against transformation logic.
3. On merge to `main`, **Azure DevOps CD** deploys the bundle to **Dev** (`databricks bundle deploy -t dev`), triggers a smoke-test pipeline run, and waits for a **manual approval** gate before promoting to **Staging**, then another gate before **Production**.
4. In **Production**, the deployed **Databricks Job** runs on its CRON schedule: it triggers the **Lakeflow Declarative Pipeline** (Bronze → Silver → Gold OBT → Star Schema), which internally resolves table dependencies, applies watermarking, runs `AUTO CDC` for dimension updates, and enforces data quality expectations.
5. On pipeline success, the job's downstream task **refreshes dependent BI aggregates/dashboards**; on failure, an **alert** fires to the team, and the engineer can go straight from the alert to the **pipeline event log / Runs UI** to see exactly which table/expectation failed and "navigate to code" from there.
6. The **entire path is auditable**: which Git commit is live in each environment, which pipeline run processed which data, and which job run triggered it — all traceable through Unity Catalog lineage plus the Azure DevOps deployment history.

**Questions this walkthrough is built to pre-empt:**
- *"How do you deploy changes without breaking production?"* → CI validation + staged environments + manual approval gates via Azure DevOps.
- *"How is this scheduled, and what happens if it fails?"* → Databricks Job with CRON trigger, retries, and failure alerting, decoupled from the pipeline's own table-level dependency resolution.
- *"How do you know a report is built on complete/correct data?"* → Job task dependencies ensure downstream refreshes only run after the pipeline succeeds; expectations gate bad data before it ever reaches Gold.
- *"How do you control cost?"* → Job-scoped compute (spins up/down per run) instead of an always-on cluster, plus `max_concurrent_runs` to avoid wasted overlapping runs.
- *"How do you promote a change from dev to prod safely?"* → Same Asset Bundle definition, different `target` (dev/staging/prod), deployed through gated Azure DevOps stages — no manual reconfiguration between environments.

---

## 📈 Business Outcomes

| Outcome | How this pipeline delivers it |
|---|---|
| **Single source of truth for trip analytics** | Gold star schema (`fact_trips` + `dim_*`) replaces scattered ad hoc queries against raw/Bronze data |
| **Accurate point-in-time reporting** | SCD Type 2 dimensions mean a report on a trip from 3 months ago reflects the customer/driver/vehicle attributes *as they were then*, not today's values |
| **Faster detection of data issues** | Expectation pass/fail counts and pipeline failure notifications surface problems within a single pipeline run instead of being discovered downstream in a stakeholder's dashboard |
| **Lower ongoing maintenance burden** | Declarative `AUTO CDC`/expectations/append-flow replace hundreds of lines of hand-written `MERGE`, dedup, and orchestration code per table — fewer places for bugs to hide, less code to review |
| **Reduced latency to insight** | Streaming ingestion + watermark-bounded joins mean trip data reaches Gold within minutes rather than a nightly batch cycle |
| **Auditability & governance** | Unity Catalog lineage, RBAC, and audit logs mean any table can be traced back to its source and access can be governed centrally |

---

## 💻 Code Examples

A few representative snippets from `transformations/` — full context and the "what this replaces" comparison is in [`legacy_vs_lakeflow_dlt_code_comparison.md`](./legacy_vs_lakeflow_dlt_code_comparison.md).

**Bronze ingestion — Auto Loader (`bronze_trips.py`)**
```python
import dlt

@dlt.table(name="bronze_trips")
def bronze_trips():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        # no schema location needs to be specified manually —
        # Lakeflow manages schema inference and evolution automatically
        .load("/mnt/raw/trips/")
    )
```

**Silver — watermark + data quality expectations (`silver_trips.py`)**
```python
import dlt
from pyspark.sql.functions import col

rules = {
    "valid_trip_id":     "trip_id IS NOT NULL",
    "valid_fare_amount": "fare_amount >= 0"
}

@dlt.table(name="silver_trips")
@dlt.expect_all_or_drop(rules)
def silver_trips():
    return (
        spark.readStream.table("LIVE.bronze_trips")
        .withWatermark("event_time", "10 minutes")   # bound state for downstream stateful joins
    )
```

**Gold — dimension via `AUTO CDC` (SCD Type 2) (`stg_dimensions.py` + `dimensions.py`)**
```python
import dlt
from pyspark.sql.functions import col

@dlt.view(name="stg_dim_customers")
def stg_dim_customers():
    return spark.readStream.table("LIVE.gold_obt").select(
        "customer_id", "customer_name", "customer_email",
        "customer_city", "customers_updated_datetime"
    )

dlt.create_streaming_table("dim_customers")

dlt.create_auto_cdc_flow(
    target="dim_customers",
    source="stg_dim_customers",
    keys=["customer_id"],
    sequence_by=col("customers_updated_datetime"),
    stored_as_scd_type=2      # full change history, versioned rows
)
```

**Gold — append-only fact table (`fact.py`)**
```python
import dlt

@dlt.table(name="fact_trips")
def fact_trips():
    return (
        spark.readStream.table("LIVE.gold_obt")
        .select(
            "trip_id", "customer_id", "driver_id", "vehicle_id", "payment_id",
            "distance_km", "fare_amount"    # additive numeric measures only
        )
    )
```

**Multi-source merge — append flow**
```python
import dlt

dlt.create_streaming_table("combined_events")

@dlt.append_flow(target="combined_events")
def flow_from_rider_app():
    return spark.readStream.table("LIVE.bronze_rider_events")

@dlt.append_flow(target="combined_events")
def flow_from_driver_app():
    return spark.readStream.table("LIVE.bronze_driver_events")
```

---

## 🧠 Key Concepts I Learned Building This

| Concept | What it solves |
|---|---|
| **Watermarking** | Bounds state store growth for stateful streaming joins by dropping data later than `max_event_time − threshold`, instead of retaining unbounded per-key state forever |
| **Star schema (fact/dim) vs. OBT** | Separates additive numeric measures (fact) from descriptive context (dimension) for efficient BI querying, derived from a single denormalized Gold OBT |
| **SCD Type 2 via `AUTO CDC`** | Preserves full dimension history with versioned rows, automatically — replacing ~45–55 lines of hand-written "detect change → close old row → insert new row" `MERGE` logic with ~10 declarative lines |
| **DLT Expectations** | Declarative data quality gates (`warn` / `drop` / `fail`) with automatic pass/fail metrics tracked per rule, per run — no custom metrics table needed |
| **Append Flow** | Merges multiple independent sources into a single target table/sink with per-source fault isolation, instead of one fragile `unionByName` + single stream |
| **Kafka/Event Hub streaming ingestion** | Consuming from Azure Event Hub's Kafka-compatible endpoint, parameterizing connection strings, and casting binary payloads to JSON |
| **Built-in monitoring/observability** | Event log persistence, Runs tab, and DAG-level expectation/upsert counts replace hand-rolled logging tables and alerting code |

---

## 🎤 How I'd Explain This Project in an Interview

**30-second summary:**
> "I built an end-to-end medallion pipeline on Databricks for rideshare trip data using Lakeflow Declarative Pipelines. Raw events land in Bronze via Auto Loader, get validated and watermarked in Silver to handle late-arriving streaming data, get joined against dimension snapshots into an append-only Gold OBT, and finally get modeled into a star schema — with dimension tables built through `AUTO CDC` for automated SCD Type 2 history tracking, and a fact table holding the additive trip measures. Data quality is enforced declaratively at each layer with expectations, and the whole thing is monitored through the pipeline's built-in event log rather than custom logging code."

**If asked "what was the hardest part / what did you learn most from":**
> "Understanding watermarking conceptually was the trickiest part — grasping that it's not just a timeout, but a moving boundary computed from the *maximum event time seen so far minus a threshold*, and that anything with an event time before that boundary gets dropped from stateful processing. The other big shift was realizing how much boilerplate `AUTO CDC` eliminates — I first wrote SCD Type 2 by hand with a manual diff-join and two-step close/insert `MERGE`, and seeing that collapse into a ~10-line declarative flow really clarified *why* teams adopt these managed frameworks instead of hand-rolling pipeline logic."

**If asked "how would you scale/productionize this further":**
> "I'd tune the watermark threshold against real p95/p99 lateness metrics from ingestion logs rather than a guessed value, add Z-ordering/liquid clustering on the dimension merge keys since `AUTO CDC` performs a `MERGE` per micro-batch, route data-quality failures to a dead-letter `append_flow` sink instead of silently dropping them, and wire the persisted event log into a dashboard for proactive alerting beyond the built-in failure notifications."

**If asked "how do you deploy and manage this in a real team environment":**
> "Code lives in Databricks Repos linked to Git, and deployment is handled through an Azure DevOps pipeline using Databricks Asset Bundles — the same bundle definition gets deployed to dev, staging, and prod, just pointed at different workspace/catalog targets, with manual approval gates before staging and prod. That means there's no manual UI reconfiguration between environments, every deploy is tied to a Git commit for audit purposes, and rolling back a bad change is just a Git revert plus a redeploy."

**If asked "how is this scheduled and monitored in production":**
> "The pipeline itself resolves table-level dependencies automatically, but I wrap it in a Databricks Job for pipeline-level orchestration — CRON scheduling, retries with backoff for transient failures, `max_concurrent_runs` to prevent overlapping runs, and failure alerts routed to the team. Downstream tasks, like refreshing a dependent dashboard, are set up with an explicit `depends_on` the pipeline task, so a report never refreshes against a partially-updated Gold layer. If something fails, the alert points straight to the pipeline's Runs UI and persisted event log to find the exact table or expectation that failed."

---

## 🚀 How to Run

1. Databricks Workspace (Repos) or import the files directly.
2. Create a new **Lakeflow Declarative Pipeline**, pointing its source code path at `transformations/`.
3. Set pipeline parameters under **Settings → Configuration** (e.g. source paths, catalog/schema names, any connection strings — never hardcode secrets in code).
4. Set the target **catalog/schema** (Unity Catalog) for the pipeline output.
5. Run in **Development** mode first to validate the DAG and expectation results; switch to a scheduled **Job** task for production runs.
6. (Optional) Enable **Event Log** persistence under pipeline settings for custom downstream monitoring/alerting.

---



## 🔭 Future Improvements

- [ ] Add Kafka/Event Hub as a live streaming source instead of file-based Auto Loader ingestion
- [ ] Add dead-letter `append_flow` sink for records failing data quality expectations
- [ ] Add Z-ordering/liquid clustering on dimension merge keys for `AUTO CDC` performance at scale
- [ ] Build a downstream alerting dashboard off the persisted pipeline event log
- [ ] Add unit tests for transformation logic using `chispa`/`pytest`, wired into the Azure DevOps `Validate` stage
- [ ] Add automated integration tests in the Dev environment stage (post-deploy smoke test assertions on row counts/expectation results, not just a successful run)
- [ ] Add blue/green or shadow-run deployment strategy for zero-downtime pipeline upgrades in prod


---

<details>
    

# RIDESHARE PROJECT - 

- Ingestion Design
- Bronze Layer - PySpark - All Table
- Silver Layer - LDP - Lakeflow Declarative Pipeline - Trips Table
- Gold Layer - OBT append - LDP  - Trips Table for joins add from dimension table
- Star Schema - Dimensional Data Model - All facts and dims


<img width="1303" height="634" alt="image" src="https://github.com/user-attachments/assets/f5484741-d324-4751-b2b9-0c0bb022680e" />

# Project
  
<img width="562" height="577" alt="image" src="https://github.com/user-attachments/assets/d18dc42e-9b10-44a6-b621-882d89844b94" />

<img width="1156" height="434" alt="image" src="https://github.com/user-attachments/assets/60e0c00c-afcc-4a09-8531-f45e0f181a11" />

- External Location to save the table

<img width="1309" height="431" alt="image" src="https://github.com/user-attachments/assets/2055df33-74ae-4590-9cb0-eb8418898baa" />
<img width="1342" height="421" alt="image" src="https://github.com/user-attachments/assets/1a18d510-1817-4364-894d-b8f80196b4e1" />

# Bronze Layer 

<img width="1364" height="596" alt="image" src="https://github.com/user-attachments/assets/6915fb8a-d5b0-4311-b75e-c4063f10234c" />
<img width="1291" height="453" alt="image" src="https://github.com/user-attachments/assets/0ada2898-234f-4d72-9360-d4105c0fcacc" />


## Pipeline - LDP - Fully Managed by Databricks
- Use Autoloader - drag everything
- No Need Checkpoint Location and Schema Location - Auto done

<img width="1325" height="517" alt="image" src="https://github.com/user-attachments/assets/cf9327f4-61d7-4546-ba78-9909ca78647a" />
<img width="1325" height="475" alt="image" src="https://github.com/user-attachments/assets/b56d01a9-b39c-4f93-b9ea-b6aede4d91fd" />

<img width="978" height="400" alt="image" src="https://github.com/user-attachments/assets/7143664f-a547-484b-a5cc-fa298c7e899d" />

##
explore
<img width="900" height="574" alt="image" src="https://github.com/user-attachments/assets/426a8f62-2451-4962-8997-6a34330feb4d" />


## Silver
<img width="1314" height="586" alt="image" src="https://github.com/user-attachments/assets/9193f0b8-2636-458a-94c0-6972af56f30d" />
<img width="1343" height="460" alt="image" src="https://github.com/user-attachments/assets/41beb9c8-fd72-40c4-b4ea-2540ee0abadb" />

## OBT_Gold

<img width="1300" height="590" alt="image" src="https://github.com/user-attachments/assets/049326a1-cbbd-4d7f-8c89-17a249f19447" />
<img width="1313" height="625" alt="image" src="https://github.com/user-attachments/assets/8e816608-f441-44d5-bcd7-a5ad425e2fe6" />
<img width="1333" height="441" alt="image" src="https://github.com/user-attachments/assets/201869b9-eb2f-4889-a177-d281c98fa7a0" />

- exploration

<img width="1009" height="452" alt="image" src="https://github.com/user-attachments/assets/3c595757-79a6-4a1e-b4eb-65c0c2b10c0c" />
<img width="1262" height="216" alt="image" src="https://github.com/user-attachments/assets/15612231-2635-4f3e-85c0-c6f661c5feb0" />
<img width="1296" height="550" alt="image" src="https://github.com/user-attachments/assets/02ec63ed-3cf9-43e0-99ee-efc4e93c4c0b" />


- Stateful transformation like join with the streaming data (state is not persisted) apply this watermark in data - treat this data - process late arrival data 10 mins (come within 10 mins not process the data - Spark Structured Streaming handle all the data which is compatible) data depending on our need and do not keep this data in your state before that. 


## Star Schema - SCD - DIM Stagging View - Dims tables
- In stagging layer we decide what columns we need for our dims.py file as OBT has all (either can create view or table ) here incremental streaming view (recommended to create view - no need store the stagging) - It is only select no transformation
  
<img width="1361" height="575" alt="image" src="https://github.com/user-attachments/assets/0172942d-7e4a-4b1a-973b-634d07bd7d86" />

<img width="1312" height="630" alt="image" src="https://github.com/user-attachments/assets/143dcfff-8635-4ba0-850e-6bd2a6cd3008" />


- external context dim and added internal context fact table some dim columns
  
<img width="1349" height="574" alt="image" src="https://github.com/user-attachments/assets/07fdae3c-0703-471d-92df-81dfa5f9ade0" />

<img width="1303" height="634" alt="image" src="https://github.com/user-attachments/assets/084dbaa8-ad98-40d9-95ea-a08e88ff55ec" />

<img width="1292" height="575" alt="image" src="https://github.com/user-attachments/assets/37195605-aba4-4549-a014-060850a312ca" />

## Fact Tables

<img width="1307" height="594" alt="image" src="https://github.com/user-attachments/assets/56536b60-13cc-42cc-92b2-c039481632bf" />
<img width="1247" height="536" alt="image" src="https://github.com/user-attachments/assets/1d6e0c5b-f087-46e7-b4a0-5a4a3c46d6d3" />

- fetching the data used join fact n cust dim

<img width="1252" height="582" alt="image" src="https://github.com/user-attachments/assets/cc1a747d-2fee-438d-8659-3800e083fd76" />

- ## Incremental Update

<img width="560" height="433" alt="image" src="https://github.com/user-attachments/assets/a66101b3-94c3-4d93-bb5b-aae7439335b7" />

<img width="1258" height="518" alt="image" src="https://github.com/user-attachments/assets/5e84879e-e27c-42d7-a5d7-726f95c967cd" />

<img width="1026" height="385" alt="image" src="https://github.com/user-attachments/assets/c3c3acbe-9c16-4e91-8bf8-de402e853807" />
<img width="1256" height="525" alt="image" src="https://github.com/user-attachments/assets/7d1bae1f-064b-4721-beb2-ee36d606dc3e" />

-- updated in streaming ingestion -- for customer id 2








</details>


---


<details>


## AZURE SETUP

<img width="1361" height="545" alt="image" src="https://github.com/user-attachments/assets/0befb2f6-8e80-4654-914d-a77d75782134" />

<img width="609" height="617" alt="image" src="https://github.com/user-attachments/assets/f5464fac-a868-4563-9f4a-4ec7ad4bebfd" />
<img width="1354" height="546" alt="image" src="https://github.com/user-attachments/assets/b45904b0-82a7-4e48-be0c-4fecfde8d48d" />
<img width="1359" height="611" alt="image" src="https://github.com/user-attachments/assets/c2915952-3388-418c-b513-af16283d93df" />
<img width="1043" height="593" alt="image" src="https://github.com/user-attachments/assets/616ab8c2-6a18-408c-a2ac-a18fee7f4441" />
<img width="1340" height="594" alt="image" src="https://github.com/user-attachments/assets/6ef4380b-f648-4c37-b292-7c3c6e40b81a" />
<img width="1358" height="444" alt="image" src="https://github.com/user-attachments/assets/dc069395-2047-4085-a81d-2aa326313383" />


<details>
  

----

## DATABRICKS SETUP

- from account console - Set UP - user management - with admin account

  <img width="1355" height="599" alt="image" src="https://github.com/user-attachments/assets/e987f0dc-7aff-4f54-a96a-7e17f1f397a3" />
<img width="1354" height="482" alt="image" src="https://github.com/user-attachments/assets/b0301c8e-fa1f-4ef7-989c-d018a71276d0" />
<img width="979" height="185" alt="image" src="https://github.com/user-attachments/assets/c317c424-0256-4935-8a25-f0288e5dae95" />
<img width="1350" height="567" alt="image" src="https://github.com/user-attachments/assets/8b1897fe-d277-4bad-a6ce-a2a5b08b6b94" />
<img width="1359" height="455" alt="image" src="https://github.com/user-attachments/assets/994c3114-c795-4e69-8296-6fad4d303e7b" />

- New metastore created with adls path and access connector after role assignments

- ## EXTERNAL LOCATIONS

<img width="1332" height="590" alt="image" src="https://github.com/user-attachments/assets/4736df9f-e26a-49e3-ad8f-69ee8d4967c8" />
<img width="1322" height="570" alt="image" src="https://github.com/user-attachments/assets/34415d5e-2fb7-4b00-9a24-2e3ac5a7643d" />
<img width="1304" height="520" alt="image" src="https://github.com/user-attachments/assets/4389d7f1-1825-4209-907c-da62aba9abd8" />
<img width="1324" height="542" alt="image" src="https://github.com/user-attachments/assets/9de0b1e1-a423-48f3-906c-ee22c4aeced1" />

</details>


# Compute



# 




</details>













