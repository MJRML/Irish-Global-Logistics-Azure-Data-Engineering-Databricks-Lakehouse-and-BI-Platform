# Irish Global Logistics -- Azure Data Engineering, Databricks Lakehouse and BI Platform

An end-to-end Azure data engineering portfolio project demonstrating
incremental ingestion, cloud data lake storage, a Databricks medallion
architecture, data quality, change data capture, environment-aware
deployment and workflow orchestration.

> **Project data:** This is a portfolio project built around a fictional
> Irish logistics business using synthetic data. No real customer or
> employee information is intended to be represented.

------------------------------------------------------------------------

## Table of contents

-   [Project overview](#project-overview)
-   [Business context and objectives](#business-context-and-objectives)
-   [Architecture](#architecture)
-   [Technology stack](#technology-stack)
-   [Source data and ingestion paths](#source-data-and-ingestion-paths)
-   [Azure Data Factory: incremental
    ingestion](#azure-data-factory-incremental-ingestion)
-   [ADLS Gen2: landing structure](#adls-gen2-landing-structure)
-   [Databricks lakehouse](#databricks-lakehouse)
    -   [Bronze layer](#bronze-layer)
    -   [Silver layer](#silver-layer)
    -   [Gold layer](#gold-layer)
-   [Databricks Asset Bundles](#databricks-asset-bundles)
-   [End-to-end orchestration](#end-to-end-orchestration)
-   [Deployment and authentication](#deployment-and-authentication)
-   [Validation and execution
    evidence](#validation-and-execution-evidence)
-   [Repository structure](#repository-structure)
-   [Security and operational
    considerations](#security-and-operational-considerations)
-   [Future enhancements](#future-enhancements)

------------------------------------------------------------------------

## Project overview

The Irish Global Logistics platform brings operational logistics data
from multiple ingestion paths into an Azure-based lakehouse. Azure Data
Factory (ADF) moves data into Azure Data Lake Storage Gen2 (ADLS Gen2),
and Azure Databricks processes it through Bronze, Silver and Gold
layers. Databricks SQL and downstream BI tools can consume the curated
Gold datasets.

The project demonstrates the engineering lifecycle from source ingestion
through transformation, quality management, deployment and
orchestration.

### Key capabilities

-   Incremental extraction from Azure SQL using modification timestamps
    and ADLS-hosted watermarks.
-   A separate file-ingestion path for employee-uploaded driver
    working-hours data.
-   ADLS Gen2 as the central landing and lakehouse storage layer.
-   Databricks Auto Loader for incremental file discovery and ingestion.
-   Delta tables and a medallion architecture.
-   Silver-layer validation, quarantine, deduplication and
    referential-integrity checks.
-   Auto CDC / SCD Type 1 processing in the Silver layer.
-   Three independent Gold reporting pipelines.
-   Databricks Asset Bundles (DABs) for repeatable deployment to
    development and production targets.
-   A single orchestrated workflow that runs Bronze, Silver and the
    three Gold pipelines in dependency order.

------------------------------------------------------------------------

## Business context and objectives

The fictional logistics operation manages depots, vehicles, drivers and
deliveries. Driver working-hours data is supplied separately through an
employee file-upload process.

The platform is designed to support questions such as:

-   How many deliveries are being handled, and what are their current
    statuses?
-   Which depots handle the greatest delivery volumes?
-   How many delivery failures occur across depots and reporting
    periods?
-   How do drivers perform across their assigned deliveries?
-   How can working-hours information be made available alongside
    operational delivery data?

The project deliberately keeps the analytical scope manageable. For
example, it does **not** calculate delivery duration from
`modified_date`: that field was not established as a reliable
delivery-completion timestamp.

------------------------------------------------------------------------

## Architecture

### End-to-end platform

``` mermaid
flowchart LR
    subgraph Sources["Source systems and files"]
        SQL["Azure SQL Database<br/>depots, vehicles, drivers, deliveries"]
        Upload["Employee-uploaded<br/>driver working-hours CSV"]
    end

    subgraph Ingestion["Ingestion"]
        ADFSQL["Azure Data Factory<br/>incremental SQL ingestion"]
        BlobADF["Azure Data Factory<br/>employee file movement"]
    end

    ADLS["Azure Data Lake Storage Gen2<br/>landing container"]

    subgraph Lakehouse["Azure Databricks lakehouse"]
        Bronze["Bronze<br/>Auto Loader and Delta"]
        Silver["Silver<br/>validation, quarantine,<br/>deduplication and Auto CDC"]
        Gold["Gold<br/>delivery, depot and<br/>driver performance"]
    end

    BI["Databricks SQL<br/>and downstream BI"]

    SQL --> ADFSQL
    Upload --> BlobADF
    ADFSQL --> ADLS
    BlobADF --> ADLS
    ADLS --> Bronze
    Bronze --> Silver
    Silver --> Gold
    Gold --> BI
```

### Medallion architecture

``` mermaid
flowchart TD
    L["ADLS Gen2 landing files"]

    subgraph B["Bronze layer"]
        B1["Incremental file ingestion"]
        B2["Schema tracking and rescue data"]
        B3["Raw Delta tables"]
    end

    subgraph S["Silver layer"]
        S1["Standardisation and validation"]
        S2["Data quality expectations"]
        S3["Quarantine invalid records"]
        S4["Deduplication and referential integrity"]
        S5["Auto CDC / SCD Type 1 where applicable"]
        S6["Curated Silver tables"]
    end

    subgraph G["Gold layer"]
        G1["gold_delivery_performance"]
        G2["gold_depot_performance"]
        G3["gold_driver_performance"]
    end

    L --> B1 --> B2 --> B3
    B3 --> S1 --> S2
    S2 --> S3
    S2 --> S4 --> S5 --> S6
    S6 --> G1
    S6 --> G2
    S6 --> G3
```

------------------------------------------------------------------------

## Technology stack

  -----------------------------------------------------------------------
  Area                    Technology              Role
  ----------------------- ----------------------- -----------------------
  Operational source      Azure SQL Database      Relational source
                                                  tables

  Orchestration and       Azure Data Factory      Incremental SQL
  extraction                                      ingestion and
                                                  employee-file movement

  Data lake               Azure Data Lake Storage Landing and lakehouse
                          Gen2                    storage

  Data engineering        Azure Databricks        Distributed processing
                                                  and lakehouse
                                                  transformations

  Storage format          Delta Lake              Transactional tables
                                                  for the lakehouse

  File ingestion          Databricks Auto Loader  Incremental discovery
                                                  and ingestion of new
                                                  files

  Transformation          PySpark and Spark SQL   Data preparation and
                                                  business
                                                  transformations

  Data governance         Unity Catalog           Catalog and schema
                                                  organisation

  Declarative pipelines   Lakeflow Declarative    Managed Silver and Gold
                          Pipelines               pipeline processing

  Deployment              Databricks Asset        Environment-aware
                          Bundles                 resource deployment

  Source control          Git and GitHub          Version control and
                                                  project documentation

  Analytics               Databricks SQL / BI     Consumption of Gold
                          tools                   reporting datasets
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## Source data and ingestion paths

The project uses two distinct ingestion paths. They converge in the ADLS
Gen2 landing container before Databricks processing.

### 1. Azure SQL operational data

The Azure SQL database contains four operational tables:

  Source table     Initial synthetic records
  -------------- ---------------------------
  `depots`                                 5
  `vehicles`                              50
  `drivers`                               40
  `deliveries`                           500
  **Total**                          **595**

ADF uses SQL authentication for the Azure SQL linked service. The
pipeline copies changed records into ADLS Gen2 as timestamped CSV files.

### 2. Employee-uploaded driver working-hours data

Driver working-hours data follows a separate file-based process:

1.  An employee CSV is uploaded to a dedicated Blob Storage container.
2.  An ADF Copy Data activity moves/combines the uploaded file data into
    ADLS Gen2.
3.  Databricks ingests the landed file into Bronze.
4.  Silver processing validates and curates the working-hours records.

The employee-file path is separate from the Azure SQL incremental
extraction pipeline. Its ADF copy activity was configured with **Merge
files** for the folder-to-file scenario.

------------------------------------------------------------------------

## Azure Data Factory: incremental ingestion

The Azure SQL ingestion pipeline is named:

`pl_ingest_azure_sql_to_adls_global_logistics_dev`

It uses a repeated watermark pattern for deliveries, depots, vehicles
and drivers. The detailed example below describes the deliveries branch;
the same general pattern is applied to the other three tables with their
corresponding source datasets, watermark files and timestamp columns.

### Watermark-based extraction pattern

``` mermaid
flowchart TD
    A["Read current deliveries watermark<br/>from ADLS Gen2"]
    B["Lookup:<br/>lookup_deliveries_watermark"]
    C["Copy Data:<br/>copy_deliveries_to_adls"]
    D["Azure SQL query:<br/>modified_date greater than watermark"]
    E["Write timestamped CSV<br/>to ADLS Gen2"]
    F["Lookup maximum modified_date<br/>for the extracted range"]
    G{"Is max_modified_date<br/>not empty?"}
    H["Copy updated watermark<br/>to ADLS Gen2"]
    I["Do not update watermark"]

    A --> B --> C --> D --> E
    C --> F --> G
    G -->|Yes| H
    G -->|No| I
```

### Step 1: read the existing watermark

The deliveries watermark Lookup activity is named
`lookup_deliveries_watermark`.

It reads the file:

`landing/watermarks/deliveries_watermark.txt`

The Lookup uses the ADLS Gen2 linked service and a DelimitedText
dataset. **First row only** is enabled. The file has no header, so the
current watermark is read from:

``` text
activity('lookup_deliveries_watermark').output.firstRow.Prop_0
```

### Step 2: extract changed records

The Copy Data activity is named `copy_deliveries_to_adls`. It uses a SQL
query against `dbo.deliveries` and selects records whose `modified_date`
is later than the current watermark:

``` sql
SELECT *
FROM dbo.deliveries
WHERE modified_date > '@{activity('lookup_deliveries_watermark').output.firstRow.Prop_0}'
```

The source uses query mode. The configured query timeout is 120 minutes,
with no source partitioning.

The sink writes CSV files to ADLS Gen2. The sink uses **Flatten
hierarchy**, **Quote all text**, and the `.csv` extension. A dynamic
file name expression creates a unique timestamped name for each run:

``` text
@concat('deliveries_', formatDateTime(utcNow(), 'yyyyMMdd_HHmmss'), '.csv')
```

An example generated file name is:

`deliveries_20261120_114101.csv`

This keeps each extraction as a separate landed file rather than reusing
a fixed output filename.

### Step 3: find the maximum extracted timestamp

The `lookup_max_deliveries_watermark` Lookup calculates the maximum
`modified_date` in the same incremental range:

``` sql
SELECT MAX(modified_date) AS max_modified_date
FROM dbo.deliveries
WHERE modified_date > '@{activity('lookup_deliveries_watermark').output.firstRow.Prop_0}'
```

The Lookup is configured for first-row output and returns
`max_modified_date`.

### Step 4: conditionally update the watermark

The If Condition activity is named `if_deliveries_watermark_update`. Its
expression checks whether the maximum timestamp is present:

``` text
@not(empty(activity('lookup_max_deliveries_watermark').output.firstRow.max_modified_date))
```

If the value is not empty, the true branch executes
`copy_update_deliveries_watermark` to update the deliveries watermark
file in ADLS Gen2. If no maximum timestamp is returned, the false branch
does not update the watermark.

This conditional update prevents the watermark from being advanced when
the extraction has no new maximum timestamp.

> The watermark update is implemented as an ADF Copy activity targeting
> the same watermark dataset/file. The README intentionally does not
> reproduce an unverified source query or claim a specific overwrite
> setting.

### Other Azure SQL branches

The deliveries pattern is reused for:

-   `depots`
-   `vehicles`
-   `drivers`

Each branch uses its own source dataset and watermark file. The four
branches are independent parts of the same ADF pipeline.

### Incremental ingestion test

An incremental test was performed by adding one new synthetic record to
each of the four Azure SQL tables, respecting the parent/child loading
order:

-   Depot: record 6
-   Vehicle: record 51
-   Driver: record 41
-   Delivery: record 526

The ADF execution completed with 20 activities succeeded, and the new
delivery file landed in ADLS Gen2. The corresponding watermark process
was exercised as part of the pipeline.

### Employee working-hours file movement

The employee upload process uses a separate ADF Copy Data activity
named:

`copy_driver_working_hours_to_adls`

The sink uses **Merge files** for the folder/wildcard-to-file copy
scenario. A landed example file was:

`driver_working_hours_20261111_082105.csv`

------------------------------------------------------------------------

## ADLS Gen2: landing structure

The storage account used for the project is `stgloballogisticsdev`. The
two containers created for the platform are:

-   `landing` --- source files and watermark files.
-   `databricks` --- Databricks-related storage locations.

The landing structure includes the following paths:

``` text
landing/
├── azure_sql/
│   └── deliveries/
│       └── dbo.deliveries.csv
├── deliveries/
├── depots/
├── vehicles/
├── drivers/
├── driver_working_hours/
└── watermarks/
    ├── deliveries_watermark.txt
    ├── depots_watermark.txt
    ├── vehicles_watermark.txt
    └── drivers_watermark.txt
```

The tree above describes the logical organisation of the landing data
and watermark files. Timestamped extraction files are generated by ADF
for incremental loads.

------------------------------------------------------------------------

## Databricks lakehouse

Unity Catalog separates development and production data:

  Environment   Catalog            Schemas
  ------------- ------------------ ----------------------------
  Development   `logistics_dev`    `bronze`, `silver`, `gold`
  Production    `logistics_prod`   `bronze`, `silver`, `gold`

Development is the default DAB target. The same code and resource
definitions can be deployed to production with the production catalog
variable.

### Bronze layer

The Bronze layer ingests the landed source files into Delta tables. The
project has five Bronze ingestion notebooks:

  -------------------------------------------------------------------------------------------------------------------
  Task                            Source notebook                                             Input
  ------------------------------- ----------------------------------------------------------- -----------------------
  `ingest_deliveries`             `src/bronze/azure_sql_db/ingest_deliveries.py`              Azure SQL deliveries
                                                                                              files

  `ingest_depots`                 `src/bronze/azure_sql_db/ingest_depots.py`                  Azure SQL depots files

  `ingest_drivers`                `src/bronze/azure_sql_db/ingest_drivers.py`                 Azure SQL drivers files

  `ingest_vehicles`               `src/bronze/azure_sql_db/ingest_vehicles.py`                Azure SQL vehicles
                                                                                              files

  `ingest_driver_working_hours`   `src/bronze/employee_data/ingest_driver_working_hours.py`   Employee working-hours
                                                                                              files
  -------------------------------------------------------------------------------------------------------------------

A shared ingestion helper is maintained in:

`src/05_utils/bronze_ingestion.py`

The ingestion design uses Databricks Auto Loader (`cloudFiles`) to
discover newly landed files incrementally. It uses explicit source
schemas and schema tracking/checkpoint locations. The CSV ingestion
configuration includes schema evolution mode `rescue`, allowing
unexpected fields or values to be captured in `_rescued_data` rather
than silently discarded.

The ingestion process also retains ingestion metadata, including:

-   `_ingestion_timestamp`
-   Source file path information, using the supported
    `_metadata.file_path` metadata field.

The pipelines use the `availableNow` trigger so that newly available
files can be processed in a bounded run. This supports incremental file
processing without requiring a continuously running stream.

#### Bronze processing flow

``` mermaid
flowchart TD
    A["ADLS Gen2 landing files"]
    B["Auto Loader / cloudFiles"]
    C["Schema and checkpoint tracking"]
    D["Rescue unexpected data<br/>where applicable"]
    E["Add ingestion metadata"]
    F["Write Bronze Delta tables"]

    A --> B --> C --> D --> E --> F
```

Auto Loader checkpoint and schema locations support incremental
discovery and schema tracking across runs. The Bronze layer retains
source-oriented data for downstream quality checks and transformations.

### Silver layer

The Silver layer turns Bronze data into validated, deduplicated and
business-consumable datasets.

The implementation uses Databricks Lakeflow Declarative Pipelines with
the Python pipelines API:

``` python
from pyspark import pipelines as dp
```

The Silver implementation includes:

1.  Validation of required fields and business rules.
2.  Data quality expectations using `@dp.expect` and `@dp.expect_all`.
3.  Separate quarantine outputs for invalid records.
4.  Explicit quarantine reason codes.
5.  Deduplication using business keys and window functions.
6.  Referential-integrity checks between related datasets.
7.  Auto CDC / SCD Type 1 processing for applicable datasets.

#### Validation and expectations

For depots, validation includes required fields such as:

-   `depot_id`
-   `depot_name`
-   `city`
-   `county`

The pipeline creates validated and quarantine outputs. Quarantine reason
codes include:

-   `MISSING_DEPOT_ID`
-   `MISSING_DEPOT_NAME`
-   `MISSING_CITY`
-   `MISSING_COUNTY`
-   `DUPLICATE_DEPOT_ID`

Vehicles are also checked against the depot data to ensure that their
depot reference exists. Similar validation and relationship checks are
applied to the other datasets where appropriate.

#### Deduplication

Deduplication uses a window over the relevant business key, ordering
records by the latest ingestion timestamp and source file as a
tie-breaker. For example, the depot deduplication key is `depot_id`.

The implementation uses `_metadata.file_path` / `_source_file` for
source-file ordering. This avoids relying on `input_file_name()` in
Unity Catalog contexts where that function is unsupported.

#### Quarantine

Invalid records are separated from the clean Silver datasets rather than
being mixed into the curated outputs. Quarantine datasets retain records
that fail validation, along with reason information to support
investigation.

#### Auto CDC and SCD Type 1

Auto CDC is used in Silver for applicable datasets, including deliveries
and driver working-hours data. The purpose is to apply incoming changes
by business key and retain the latest applicable record using sequencing
information, implementing SCD Type 1 behaviour where configured.

This provides a managed way to process changes arriving in later source
files without treating every incoming record as an unrelated new
business entity.

> The exact Auto CDC function arguments and sequencing expression are
> intentionally not reproduced here because the final source code was
> not available for direct verification during README preparation. The
> description above reflects the implemented project approach without
> inventing a code listing.

#### Silver pipeline outputs

The Silver pipeline is configured as a Lakeflow Declarative Pipeline
targeting the `silver` schema. Its DAB configuration uses the
environment-specific catalog variable, serverless compute and Photon.

A recorded pipeline execution showed 17 tables in the pipeline,
including validated, clean and quarantine outputs. Example execution
results included:

  Output                                Observed records
  ----------------------------------- ------------------
  `deliveries_clean`                                   1
  `deliveries_quarantine`                              0
  `deliveries_validated`                               1
  `depots`                                             6
  `depots_quarantine`                                  0
  `depots_validated`                                   6
  `driver_working_hours_quarantine`                    8

These are execution-specific observations, not fixed expected counts for
every run.

### Gold layer

The Gold layer contains three independent reporting pipelines. Each
pipeline produces a business-oriented dataset for a distinct analytical
requirement.

The pipelines are separately defined and can be refreshed independently
after Silver processing has completed.

#### 1. Delivery performance

Gold dataset:

`gold_delivery_performance`

The delivery-performance pipeline reads these Silver datasets:

-   `silver_deliveries`
-   `silver_depots`
-   `silver_drivers`
-   `silver_vehicles`

It joins deliveries to their related depot, driver and vehicle records
using the corresponding identifiers, including `depot_id`, `driver_id`
and `vehicle_id`.

The output contains business-ready delivery attributes and reporting
fields. Derived fields include:

-   `delivery_year`
-   `delivery_month`
-   `delivery_day`
-   `delivery_month_year`
-   `delivery_success_flag`
-   `delivery_failure_flag`
-   `delivery_completed_flag`
-   `delivery_delayed_flag`
-   `delivery_in_transit_flag`
-   `delivery_pending_flag`

The design deliberately excludes delivery duration and
hours-per-delivery calculations because `modified_date` was not
confirmed to represent the actual completion timestamp.

A recorded Gold execution produced **526 rows** in
`gold_delivery_performance`.

#### 2. Depot performance

Gold dataset:

`gold_depot_performance`

The depot-performance pipeline uses Silver delivery and depot data,
together with driver working-hours information. It supports depot-level
operational reporting, including monthly delivery workload and failure
analysis.

The output is designed to help answer questions such as:

-   How many deliveries does each depot handle by month?
-   How does depot workload change over time?
-   How many delivery failures are associated with each depot and
    reporting period?

A recorded execution produced **55 rows**. The pipeline was configured
for full recomputation in the observed run.

#### 3. Driver performance

Gold dataset:

`gold_driver_performance`

The driver-performance pipeline uses:

-   `silver_deliveries`
-   `silver_driver_working_hours`
-   `silver_drivers`

It creates a reporting-ready dataset combining driver information,
delivery outcomes and working-hours information for driver-level
analysis.

A recorded execution produced **290 rows**. The pipeline was configured
for full recomputation in the observed run.

#### Gold pipeline summary

  Gold dataset                    Recorded output rows Observed processing mode
  ----------------------------- ---------------------- --------------------------
  `gold_delivery_performance`                      526 Incremental
  `gold_depot_performance`                          55 Full recompute
  `gold_driver_performance`                        290 Full recompute

These figures describe recorded executions and may change as source data
changes.

------------------------------------------------------------------------

## Databricks Asset Bundles

Databricks Asset Bundles are used to define, validate and deploy the
project's Databricks resources from source-controlled YAML
configuration.

The bundle is named:

`global_logistics`

The main configuration file is `databricks.yml`. It includes resource
definitions from:

``` yaml
include:
  - resources/*.yml
```

This keeps the bundle definition separate from the individual job and
pipeline resources.

### Bundle targets and environment separation

The bundle defines two targets:

  Setting            DEV               PROD
  ------------------ ----------------- ------------------
  Target             `dev`             `prod`
  Deployment mode    Development       Production
  Catalog variable   `logistics_dev`   `logistics_prod`
  Schedule status    `PAUSED`          `UNPAUSED`
  Default target     Yes               No

Both targets use the same Azure Databricks workspace, but use separate
catalogs and deployment configurations.

The DEV target is the default and uses development deployment mode. The
PROD target uses production deployment mode and a dedicated bundle
deployment root. Personal workspace paths and user-specific identifiers
are deliberately omitted from this public documentation.

### Resource configuration files

The bundle includes these resource definitions:

  ----------------------------------------------------------------------------------------
  File                                                 Purpose
  ---------------------------------------------------- -----------------------------------
  `resources/bronze_job.job.yml`                       Defines the five Bronze notebook
                                                       tasks

  `resources/silver_pipeline.pipeline.yml`             Defines the Silver Lakeflow
                                                       pipeline

  `resources/gold_delivery_performance.pipeline.yml`   Defines the delivery Gold pipeline

  `resources/gold_depot_performance.pipeline.yml`      Defines the depot Gold pipeline

  `resources/gold_driver_performance.pipeline.yml`     Defines the driver Gold pipeline

  `resources/orchestration_job.job.yml`                Defines the end-to-end workflow
  ----------------------------------------------------------------------------------------

### Environment-aware catalog configuration

The resource definitions use a bundle variable rather than hardcoding
the catalog:

``` yaml
catalog: ${var.catalog}
```

The Bronze tasks pass the same variable to their notebooks:

``` yaml
base_parameters:
  catalog: ${var.catalog}
```

The Silver and Gold pipelines also receive the catalog through their
pipeline configuration. This allows the same source-controlled resource
definitions to be deployed to DEV or PROD with the appropriate catalog.

### Bronze job definition

The Bronze job is named `global_logistics_bronze_ingestion`. It contains
five notebook tasks:

-   `ingest_deliveries`
-   `ingest_depots`
-   `ingest_drivers`
-   `ingest_vehicles`
-   `ingest_driver_working_hours`

The first four read the Azure SQL-derived landing data. The fifth
ingests the employee-uploaded working-hours files.

### Silver and Gold pipeline definitions

The Silver pipeline targets the `silver` schema. Its resource
configuration uses a library glob to load transformation code from its
configured transformations directory.

Each Gold pipeline targets the `gold` schema and references its own
notebook:

-   `delivery_performance`
-   `depot_performance`
-   `driver_performance`

All three Gold pipeline definitions use serverless compute and Photon.

------------------------------------------------------------------------

## End-to-end orchestration

The DAB-managed workflow is named:

`global_logistics_full_workflow`

It runs the Bronze job first, then the Silver pipeline, and finally the
three independent Gold pipelines.

``` mermaid
flowchart TD
    A["bronze_ingestion<br/>Run Bronze ingestion job"]
    B["silver_processing<br/>Run Silver pipeline"]
    C["gold_delivery_performance<br/>Run delivery Gold pipeline"]
    D["gold_depot_performance<br/>Run depot Gold pipeline"]
    E["gold_driver_performance<br/>Run driver Gold pipeline"]

    A --> B
    B --> C
    B --> D
    B --> E
```

The dependency structure ensures that:

1.  Bronze ingestion completes before Silver starts.
2.  Silver processing completes before any Gold pipeline starts.
3.  The three Gold pipelines do not depend on one another and can
    execute independently.

### Schedule

The orchestration job uses this Quartz cron expression:

``` yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "UTC"
  pause_status: ${var.job_schedule_pause_status}
```

This configures a daily run at **02:00 UTC**. The pause status is
controlled by the environment variable:

-   DEV: paused.
-   PROD: unpaused.

The Bronze job itself is invoked by the orchestration job through a
`run_job_task`. Silver and Gold are invoked through `pipeline_task`
definitions.

The task definitions use bundle resource references, such as:

``` yaml
job_id: ${resources.jobs.bronze_ingestion_job.id}
```

and:

``` yaml
pipeline_id: ${resources.pipelines.global_logistics_silver_pipeline.id}
```

This avoids hardcoding deployed job and pipeline IDs in the
orchestration YAML.

------------------------------------------------------------------------

## Deployment and authentication

### Prerequisites

Before deploying the bundle, install and configure:

-   Azure Databricks workspace access.
-   Databricks CLI.
-   A Databricks personal access token (PAT) or another supported
    authentication method.
-   Git, if cloning the repository.
-   Access to the Azure resources and Unity Catalog objects used by the
    project.

The project was developed with **Databricks CLI v1.21.0**.

### 1. Clone the repository

``` powershell
git clone https://github.com/MJRML/Irish-Global-Logistics-Azure-Data-Engineering-Databricks-Lakehouse-and-BI-Platform.git
cd Irish-Global-Logistics-Azure-Data-Engineering-Databricks-Lakehouse-and-BI-Platform
```

### 2. Verify the Databricks CLI

``` powershell
databricks --version
```

The project environment used:

``` text
Databricks CLI v1.21.0
```

### 3. Configure the authentication profile

The development authentication profile used for this project is:

`LOGISTICS_DEV`

The profile is configured locally through the Databricks CLI
authentication configuration. The workspace host and authentication
credential must be supplied securely during setup.

Do not commit PATs, passwords or other credentials to Git. Do not paste
a PAT into the README, source files or screenshots.

List configured profiles:

``` powershell
databricks auth profiles
```

### 4. Verify authentication

``` powershell
databricks current-user me --profile LOGISTICS_DEV
```

A successful response returns the authenticated Databricks account
identity and confirms that the CLI can reach the workspace.

### 5. Validate the DEV bundle

From the directory containing `databricks.yml`, run:

``` powershell
databricks bundle validate -t dev
```

A successful validation reports the bundle name, target and workspace
configuration, followed by:

``` text
Validation OK!
```

### 6. Deploy to DEV

``` powershell
databricks bundle deploy -t dev
```

The deployment command uploads bundle files and creates or updates the
resources defined by the bundle.

A recorded DEV deployment completed successfully and reported:

``` text
Files: 4 uploaded, 10 deleted
Resources: 0 created, 2 changed, 0 deleted, 4 unchanged
```

The two updated resources were the Gold driver-performance pipeline and
the orchestration job. The deployment result reflects the changes
detected during that particular deployment.

### 7. Run the DEV workflow

``` powershell
databricks bundle run global_logistics_full_workflow -t dev
```

This runs the end-to-end workflow in the development target.

### 8. Production deployment

Production is a separate target and uses the `logistics_prod` catalog.
Validate before deploying:

``` powershell
databricks bundle validate -t prod
```

Deploy only when a production release is intended:

``` powershell
databricks bundle deploy -t prod
```

The production target enables the daily orchestration schedule.
Production deployment and execution should be treated as operational
changes, not as routine development testing.

> The production deployment commands are documented for reproducibility.
> Do not run them simply to test the README.

------------------------------------------------------------------------

## Validation and execution evidence

The project was exercised at multiple stages.

### Azure Data Factory

-   The Azure SQL incremental ingestion pipeline was tested.
-   ADF successfully executed the activities in the four source-table
    branches.
-   An incremental test inserted one additional synthetic record into
    each of the four Azure SQL tables.
-   New delivery data landed in ADLS Gen2 and the watermark process was
    exercised.
-   The separate driver working-hours file movement pipeline was
    published and successfully executed.

### Bronze

The Bronze ingestion job was run successfully. Its five tasks cover
deliveries, depots, drivers, vehicles and driver working-hours data.

### Silver

The Silver pipeline completed successfully in a recorded run. Its
execution graph showed 17 tables, including validated, clean and
quarantine outputs. The observed results included quarantined driver
working-hours records, demonstrating that invalid data was separated
rather than silently included in curated outputs.

### Gold

Recorded Gold pipeline executions included:

  Pipeline                      Recorded result
  ----------------------------- --------------------
  `gold_delivery_performance`   526 output records
  `gold_depot_performance`      55 output records
  `gold_driver_performance`     290 output records

### End-to-end production workflow

The production `global_logistics_full_workflow` showed successful
executions on 19 and 20 November 2026. In the latest displayed run, all
five tasks succeeded:

1.  Bronze ingestion
2.  Silver processing
3.  Gold delivery performance
4.  Gold depot performance
5.  Gold driver performance

The latest displayed execution took 4 minutes 39 seconds. The preceding
displayed execution took 4 minutes 19 seconds.

------------------------------------------------------------------------

## Repository structure

The main project structure is organised as follows:

``` text
global_logistics/
├── databricks.yml
├── README.md
├── .gitignore
├── AGENTS.md
├── resources/
│   ├── bronze_job.job.yml
│   ├── silver_pipeline.pipeline.yml
│   ├── gold_delivery_performance.pipeline.yml
│   ├── gold_depot_performance.pipeline.yml
│   ├── gold_driver_performance.pipeline.yml
│   └── orchestration_job.job.yml
└── src/
    ├── 05_utils/
    │   ├── bronze_ingestion.py
    │   ├── depot_performance_testing.py
    │   ├── driver_performance_testing.py
    │   └── source_schemas.py
    ├── bronze/
    │   ├── azure_sql_db/
    │   │   ├── ingest_deliveries.py
    │   │   ├── ingest_depots.py
    │   │   ├── ingest_drivers.py
    │   │   └── ingest_vehicles.py
    │   └── employee_data/
    │       └── ingest_driver_working_hours.py
    ├── silver/
    │   └── transformations/
    │       └── silver.py
    └── gold/
        ├── delivery_performance.py
        ├── depot_performance.py
        └── driver_performance.py
```

The repository also contains a `.gitignore` to keep local environment
files, deployment state and other non-source artefacts out of version
control.

------------------------------------------------------------------------

## Security and operational considerations

-   **Secrets:** Never commit PATs, passwords, connection strings or
    `.env` files.
-   **Local deployment state:** Databricks bundle deployment metadata is
    local environment state and should not be committed.
-   **Environment separation:** Use `logistics_dev` for development and
    `logistics_prod` for production.
-   **Production scheduling:** The production orchestration schedule is
    enabled; the development schedule is paused.
-   **Workspace paths:** Keep personal workspace paths and account
    identifiers out of public documentation.
-   **Synthetic data:** The project uses synthetic portfolio data.
-   **Incremental processing:** Watermarks and Auto Loader
    checkpoints/schema locations support incremental processing.
-   **Data quality:** Invalid records are routed to quarantine outputs
    for visibility and follow-up.

------------------------------------------------------------------------

## Future enhancements

Potential extensions, outside the completed project scope, include:

-   Add automated data-quality reporting and alerting.
-   Add a formal automated test suite for transformation logic.
-   Add CI/CD workflows for bundle validation and deployment.
-   Add dashboards on top of the Gold datasets.
-   Extend operational monitoring for pipeline duration, failures and
    data freshness.
-   Add more source datasets or additional operational reporting
    requirements.

------------------------------------------------------------------------

## Project repository

**GitHub:** [Irish Global Logistics -- Azure Data Engineering,
Databricks Lakehouse and BI
Platform](https://github.com/MJRML/Irish-Global-Logistics-Azure-Data-Engineering-Databricks-Lakehouse-and-BI-Platform)
