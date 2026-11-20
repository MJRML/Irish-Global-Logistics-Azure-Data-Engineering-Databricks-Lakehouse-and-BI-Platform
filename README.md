# Irish Global Logistics
### Azure Data Engineering | Databricks Lakehouse | DataOps

An end-to-end Azure data platform demonstrating incremental ingestion, a Databricks medallion lakehouse, data quality, change data capture, and deployment across development and production environments.

![Azure](https://img.shields.io/badge/Azure-Cloud-blue)
![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-0078D4)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-EF3E2C)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-003366)
![Python](https://img.shields.io/badge/Python-PySpark-3776AB)

**Project:** Portfolio project · **Data:** Synthetic · **Cloud:** Microsoft Azure

---

## Project overview

This project builds a data platform for a fictional Irish logistics business. Operational data is extracted from Azure SQL Database, while driver working-hours information arrives as employee-uploaded CSV files. Azure Data Factory moves both data sources into Azure Data Lake Storage Gen2, where Azure Databricks processes the data through Bronze, Silver and Gold layers.

The aim was to build a practical end-to-end engineering solution: ingest data incrementally, retain traceability in the lakehouse, apply data-quality controls, manage changes to source records, and publish curated datasets for operational reporting. Databricks Asset Bundles provide repeatable deployment across separate development and production catalogs.

### Platform components

| Platform area | Technology / approach |
|---|---|
| Source systems | Azure SQL Database and employee-uploaded CSV files |
| Ingestion | Azure Data Factory |
| Data lake | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks, PySpark and Delta Lake |
| File ingestion | Databricks Auto Loader |
| Data architecture | Bronze → Silver → Gold |
| Data quality | Expectations, validation, quarantine and deduplication |
| Change processing | Auto CDC / SCD Type 1 for applicable Silver datasets |
| Governance | Unity Catalog |
| Deployment and orchestration | Databricks Asset Bundles |

## Architecture

```mermaid
flowchart LR
    SQL["Azure SQL Database<br/>Depots · Vehicles · Drivers · Deliveries"]
    UPLOAD["Employee-uploaded<br/>working-hours CSV"]
    ADF["Azure Data Factory"]
    ADLS["ADLS Gen2<br/>Landing"]
    BRONZE["Bronze<br/>Auto Loader + Delta"]
    SILVER["Silver<br/>Quality + CDC"]
    GOLD["Gold<br/>Performance datasets"]
    BI["Databricks SQL / BI"]

    SQL --> ADF
    UPLOAD --> ADF
    ADF --> ADLS
    ADLS --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD
    GOLD --> BI
```

**Two ingestion paths**
- **Azure SQL:** ADF extracts changed records from four operational tables using per-table watermarks.
- **Employee files:** ADF moves uploaded driver working-hours CSV files from a separate Blob Storage container into ADLS Gen2.

**Processing flow**
- Auto Loader incrementally ingests landed files into Bronze Delta tables.
- Silver applies data quality rules, deduplication, quarantine and configured CDC processing.
- Three independent Gold pipelines publish datasets for delivery, depot and driver performance.

---

## 1. Azure Data Factory

### Incremental Azure SQL ingestion

![ADF incremental ingestion pipeline](screenshots/adf/incremental_ingestion_pipeline.png)

*Four source-table branches use the same watermark-based ingestion pattern.*

**Pipeline:** `pl_ingest_azure_sql_to_adls_global_logistics_dev`

For each source table, the pipeline:

1. Reads the existing watermark from ADLS Gen2.
2. Extracts rows modified after that watermark.
3. Writes the extracted records to a timestamped CSV file.
4. Looks up the maximum modification timestamp.
5. Updates the watermark only when a new maximum timestamp is available.

**Example: deliveries source query**

```sql
SELECT *
FROM dbo.deliveries
WHERE modified_date > '@{activity('lookup_deliveries_watermark').output.firstRow.Prop_0}'
```

**Timestamped output filename**

```text
@concat('deliveries_', formatDateTime(utcNow(), 'yyyyMMdd_HHmmss'), '.csv')
```

**Watermark update condition**

```text
@not(empty(activity('lookup_max_deliveries_watermark').output.firstRow.max_modified_date))
```

### ADF configuration evidence

| Screenshot | Evidence |
|---|---|
| ![Watermark lookup](screenshots/adf/watermark_lookup_settings.png) | Read the current watermark |
| ![Watermark dataset](screenshots/adf/watermark_dataset_configuration.png) | ADLS watermark file configuration |
| ![Copy activity](screenshots/adf/deliveries_copy_activity.png) | Deliveries Copy Data activity |
| ![Copy source](screenshots/adf/deliveries_copy_source_settings.png) | SQL query-based extraction |
| ![Copy sink](screenshots/adf/deliveries_copy_sink_settings.png) | CSV sink configuration |
| ![File naming](screenshots/adf/deliveries_copy_file_naming.png) | Dynamic timestamped file name |
| ![Maximum watermark](screenshots/adf/max_watermark_lookup.png) | Retrieve maximum source timestamp |
| ![If condition](screenshots/adf/watermark_if_condition.png) | Check before updating watermark |
| ![Watermark update](screenshots/adf/watermark_update_copy_activity.png) | Watermark update activity |

### Employee working-hours files

- Separate employee-upload Blob Storage input.
- ADF activity: `copy_driver_working_hours_to_adls`.
- Copy behaviour: **Merge files**.
- The landed CSV is ingested by the Bronze working-hours notebook.

---

## 2. ADLS Gen2

**Storage account:** `stgloballogisticsdev`

| Container | Purpose |
|---|---|
| `landing` | Source extracts, employee files and watermark files |
| `databricks` | Databricks-related storage locations |

Key landing paths:

```text
landing/
├── azure_sql/
│   └── deliveries/
├── deliveries/
├── depots/
├── drivers/
├── vehicles/
├── driver_working_hours/
└── watermarks/
    ├── deliveries_watermark.txt
    ├── depots_watermark.txt
    ├── drivers_watermark.txt
    └── vehicles_watermark.txt
```

---

## 3. Databricks lakehouse

Unity Catalog provides separate development and production catalogs.

| Environment | Catalog | Schemas |
|---|---|---|
| DEV | `logistics_dev` | `bronze`, `silver`, `gold` |
| PROD | `logistics_prod` | `bronze`, `silver`, `gold` |

### Bronze — incremental ingestion

![Bronze ingestion job](screenshots/databricks/bronze_ingestion_job.png)

*Five ingestion tasks are managed through Databricks Asset Bundles.*

**Ingestion notebooks**
- `ingest_deliveries`
- `ingest_depots`
- `ingest_drivers`
- `ingest_vehicles`
- `ingest_driver_working_hours`

**Implementation**
- Shared ingestion helper: `src/05_utils/bronze_ingestion.py`.
- Auto Loader (`cloudFiles`) for incremental file discovery.
- Explicit schemas and schema tracking.
- Checkpoints to track processed files.
- Rescue mode for unexpected data in `_rescued_data`.
- Ingestion metadata: `_ingestion_timestamp` and `_metadata.file_path`.
- `availableNow` trigger for bounded processing.
- Delta tables as the Bronze storage format.

![Bronze run history](screenshots/databricks/bronze_run_history.png)

*Bronze job execution history showing successful runs.*

### Silver — data quality and CDC

![Silver pipeline overview](screenshots/databricks/silver_pipeline_overview.png)

*Silver pipeline lineage across the source and transformed datasets.*

**Processing**
- Validation using `@dp.expect` and `@dp.expect_all`.
- Separate clean, validated and quarantine outputs.
- Deduplication using business keys and window functions.
- Latest-record selection using ingestion timestamps and source-file ordering.
- Referential-integrity checks, including vehicle-to-depot validation.
- Auto CDC / SCD Type 1 for applicable datasets, including deliveries and driver working-hours data.

**Example quarantine reason codes**
- `MISSING_DEPOT_ID`
- `MISSING_DEPOT_NAME`
- `MISSING_CITY`
- `MISSING_COUNTY`
- `DUPLICATE_DEPOT_ID`

![Silver table results](screenshots/databricks/silver_table_results.png)

*Silver pipeline results show clean, validated and quarantine datasets.*

### Gold — reporting datasets

Three independent Gold pipelines publish reporting-ready materialized views. Each can be refreshed separately after Silver processing.

#### Delivery performance

![Gold delivery performance lineage](screenshots/databricks/gold_delivery_performance.png)

**`gold_delivery_performance`**
- Joins deliveries with depots, drivers and vehicles.
- Derives delivery year, month, day and month-year fields.
- Creates success, failure, completed, delayed, in-transit and pending flags.
- Does not infer delivery duration from `modified_date`.

![Gold delivery performance metrics](screenshots/databricks/gold_delivery_performance_metrics.png)

*Recorded output: 526 rows · Incremental processing.*

#### Depot performance

![Gold depot performance](screenshots/databricks/gold_depot_performance.png)

**`gold_depot_performance`**
- Monthly depot workload.
- Delivery volumes by depot.
- Delivery failure analysis.
- Combines delivery, depot and driver working-hours information.

*Recorded output: 55 rows.*

#### Driver performance

![Gold driver performance](screenshots/databricks/gold_driver_performance.png)

**`gold_driver_performance`**
- Driver-level operational reporting.
- Delivery outcomes alongside working-hours information.
- Combines deliveries, drivers and driver working-hours data.

*Recorded output: 290 rows.*

---

## 4. Deployment and orchestration

The Databricks Asset Bundle is named `global_logistics`. Resource definitions are maintained in the `resources/` directory and included by `databricks.yml`.

### DEV and PROD targets

| Setting | DEV | PROD |
|---|---|---|
| Bundle target | `dev` | `prod` |
| Deployment mode | Development | Production |
| Unity Catalog | `logistics_dev` | `logistics_prod` |
| Workflow schedule | Paused | Daily at 02:00 UTC |
| Default target | Yes | No |

The `${var.catalog}` variable passes the selected catalog into the deployed jobs and pipelines.

### End-to-end workflow

![DEV workflow dependencies](screenshots/orchestration/dev_workflow_dependencies.png)

The `global_logistics_full_workflow` contains five tasks:

```mermaid
flowchart TD
    A["bronze_ingestion"]
    B["silver_processing"]
    C["gold_delivery_performance"]
    D["gold_depot_performance"]
    E["gold_driver_performance"]

    A --> B
    B --> C
    B --> D
    B --> E
```

- Bronze runs first.
- Silver depends on Bronze.
- All three Gold pipelines depend on Silver.
- Gold tasks run independently after Silver.

![PROD workflow dependencies](screenshots/orchestration/prod_workflow_dependencies.png)

*Production workflow configuration and task dependencies.*

![PROD workflow run history](screenshots/orchestration/prod_workflow_run_history.png)

*The displayed production execution completed all five tasks successfully in 4 minutes 39 seconds.*

### Databricks CLI and bundle deployment

**CLI version:** `1.21.0`  
**Authentication profile:** `LOGISTICS_DEV`

Verify the CLI and authentication:

```powershell
databricks --version
databricks auth profiles
databricks current-user me --profile LOGISTICS_DEV
```

Validate and deploy DEV:

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Run the workflow:

```powershell
databricks bundle run global_logistics_full_workflow -t dev
```

Validate and deploy PROD when performing a controlled production release:

```powershell
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

**Recorded DEV deployment result**

```text
Validation OK!

Files: 4 uploaded, 10 deleted
Resources: 0 created, 2 changed, 0 deleted, 4 unchanged
```

The updated resources were the Gold driver-performance pipeline and the orchestration job.

---

## 5. Execution results

| Component | Recorded result |
|---|---|
| ADF incremental ingestion | 20 activities succeeded |
| Bronze | Successful job executions |
| Silver | Completed run; 17 pipeline tables |
| Gold delivery | 526 output rows |
| Gold depot | 55 output rows |
| Gold driver | 290 output rows |
| PROD orchestration | Five tasks succeeded |
| DEV bundle deployment | Validation and deployment succeeded |

*Row counts and durations are observations from recorded runs, not fixed expectations.*

---

## Repository structure

```text
global_logistics/
├── databricks.yml
├── README.md
├── .gitignore
├── resources/
│   ├── bronze_job.job.yml
│   ├── silver_pipeline.pipeline.yml
│   ├── gold_delivery_performance.pipeline.yml
│   ├── gold_depot_performance.pipeline.yml
│   ├── gold_driver_performance.pipeline.yml
│   └── orchestration_job.job.yml
├── src/
│   ├── 05_utils/
│   ├── bronze/
│   │   ├── azure_sql_db/
│   │   └── employee_data/
│   ├── silver/
│   │   └── transformations/
│   └── gold/
└── screenshots/
    ├── adf/
    ├── architecture/
    ├── databricks/
    └── orchestration/
```

## Security

- Do not commit PATs, passwords, connection strings or `.env` files.
- Use `logistics_dev` for development and reserve `logistics_prod` for controlled production deployments.
- Remove personal identifiers and workspace-specific paths from public screenshots.
- This portfolio project uses synthetic data.

## Repository

[View the project on GitHub](https://github.com/MJRML/Irish-Global-Logistics-Azure-Data-Engineering-Databricks-Lakehouse-and-BI-Platform)
