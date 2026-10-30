# Data Architecture — Dublin Rental Market Lakehouse

## 1. Project Overview

The Dublin Rental Market Lakehouse is a data engineering project designed to ingest, process, validate, and organize official Irish rental market data using a Medallion Architecture.

The solution uses Apache Spark and Delta Lake within Databricks Free Edition to transform raw statistical data into structured analytical datasets.

## 2. Architecture

The project follows the Medallion Architecture, organized into three processing layers:

### Bronze Layer — Raw Data Ingestion

- Ingests the official CSO Ireland JSON-stat dataset.
- Preserves the original observations and dimensional information.
- Converts the source data into a structured Spark DataFrame.
- Stores the ingested data as a Delta table.

### Silver Layer — Data Transformation

- Standardizes dimension labels and data types.
- Converts rental values into decimal format.
- Extracts year and quarter information.
- Creates standardized reporting dates.
- Identifies missing and valid rental observations.
- Stores the transformed dataset as a Delta table.

### Gold Layer — Analytical Data Products

Creates four analytical Delta tables:

- `gold_dublin_rent_trends`: quarterly rental trends for Dublin.
- `gold_dublin_rent_by_property_type`: rental values by property type.
- `gold_dublin_rent_by_bedrooms`: rental values by bedroom category.
- `gold_dublin_rent_by_location`: rental comparisons across Dublin geographic categories.

Geographic categories may overlap and should not be treated as additive regions.

## 3. Data Quality

Automated validation checks cover:

- Source-to-Bronze record reconciliation.
- Silver dimensional uniqueness.
- Positive rental values.
- Missing-value flag consistency.
- Valid reporting periods.
- Gold data validity.
- Reconciliation between Silver and Gold.

The current validation suite contains nine checks, all passing in the latest execution.

## 4. Technology Stack

- Databricks Free Edition
- Apache Spark
- PySpark
- Delta Lake
- Python
- SQL
- Git and GitHub

## 5. Data Source

Central Statistics Office (CSO) Ireland.

Dataset: RIQ02 — RTB Average Monthly Rent Report.

Source API:

https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/RIQ02/JSON-stat/2.0/en

## 6. Execution Environment

The project is developed and executed in Databricks Free Edition. No paid cloud infrastructure or external managed data services are required.

## 7. Current Project Status

- [x] Source data exploration
- [x] Bronze ingestion
- [x] Silver transformations
- [x] Gold analytical tables
- [x] Initial automated data quality validation
- [ ] Modular Python implementation
- [ ] Independent automated tests
- [ ] Final project documentation
