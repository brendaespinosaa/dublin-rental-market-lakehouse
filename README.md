# Dublin Rental Market Lakehouse

An end-to-end data engineering project that processes official Irish rental market data using **Databricks, Apache Spark, PySpark, Delta Lake, and the Medallion Architecture**.

The project transforms raw statistical data from Ireland's Central Statistics Office (CSO) into structured and validated analytical datasets, focusing on rental market trends in Dublin.

## Project Overview

The objective is to build a scalable and organized data processing workflow that demonstrates key data engineering concepts, including:

- Data ingestion from an official statistical API.
- Data processing and transformation with PySpark.
- Medallion Architecture implementation (Bronze, Silver, and Gold).
- Delta Lake storage.
- Data quality validation and reconciliation.
- Analytical data modeling.

## Architecture

The project follows the **Medallion Architecture**, separating data processing into three layers.

### Bronze — Raw Data Ingestion

- Ingests the official CSO Ireland JSON-stat dataset.
- Extracts observations and dimensional metadata.
- Converts the source data into a structured Spark DataFrame.
- Stores the raw observations in Delta Lake.

### Silver — Data Transformation

- Standardizes dimension labels and data types.
- Converts rental values into decimal format.
- Extracts year and quarter information.
- Creates standardized reporting dates.
- Identifies missing and valid rental observations.
- Preserves missing values without replacing them with zero.

### Gold — Analytical Data Products

Creates four analytical Delta tables:

| Dataset | Description |
|---|---|
| `gold_dublin_rent_trends` | Quarterly rental trends for Dublin. |
| `gold_dublin_rent_by_property_type` | Rental values by property type. |
| `gold_dublin_rent_by_bedrooms` | Rental values by bedroom category. |
| `gold_dublin_rent_by_location` | Rental comparisons across Dublin geographic categories. |

**Geographic consideration:** CSO location categories may overlap. Therefore, detailed geographic categories are treated as separate comparisons and must not be summed as if they were mutually exclusive regions.

## Data Source

**Central Statistics Office (CSO) Ireland**

- Dataset: RIQ02 — RTB Average Monthly Rent Report.
- Coverage: 2007 Q4 to 2025 Q4.
- Format: JSON-stat.
- Measurement: Average monthly rent in EUR.

Official API:

[CSO Ireland — RIQ02 Dataset](https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/RIQ02/JSON-stat/2.0/en)

## Key Findings

The processed dataset provides insights into Dublin's rental market.

| Metric | Result |
|---|---:|
| Source observations | 1,367,436 |
| Valid rental observations | 376,988 |
| Missing observations | 990,448 |
| Historical coverage | 2007 Q4 – 2025 Q4 |
| Dublin quarterly trend records | 73 |

### Dublin Rental Trends

The CSO aggregate category for Dublin shows the following nominal rental changes:

| Period | Average Monthly Rent |
|---|---:|
| 2007 Q4 | €1,312.69 |
| 2025 Q4 | €2,165.02 |

**Nominal growth: 64.93%**

Values are not adjusted for inflation.

### Rental Values by Property Type — 2025 Q4

| Property Type | Average Monthly Rent |
|---|---:|
| Detached house | €3,057.95 |
| Semi-detached house | €2,598.88 |
| Terrace house | €2,432.52 |
| Apartment | €2,154.18 |
| Other flats | €1,444.97 |

## Data Quality

The pipeline includes automated validation checks to ensure data consistency and reliability.

The current validation suite includes nine checks:

- Source-to-Bronze record reconciliation.
- Bronze-to-Silver record reconciliation.
- Dimensional uniqueness.
- Rental value consistency.
- Missing value flag consistency.
- Valid value flag consistency.
- Reporting period validation.
- Gold data validity.
- Silver-to-Gold reconciliation.

**Latest validation result: 9/9 checks passed.**

Independent Pytest implementation is planned as a subsequent development stage.

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks Free Edition | Development and execution environment |
| Apache Spark | Distributed data processing |
| PySpark | Data transformation |
| Delta Lake | Data storage and table management |
| Python | Data engineering logic |
| SQL | Data exploration and validation |
| Git & GitHub | Version control and documentation |

## Repository Structure

```text
dublin-rental-market-lakehouse/
│
├── data/
│
├── docs/
│   └── architecture.md
│
├── notebooks/
│   └── 01_dublin_rental_market_lakehouse.ipynb
│
├── src/
│   ├── ingestion/
│   ├── transformations/
│   └── quality/
│
├── tests/
│   └── __init__.py
│
├── .gitignore
└── README.md
```

## Execution Environment

The project is developed using Databricks Free Edition.

The current implementation runs in a Databricks notebook and uses a Unity Catalog volume to store the source JSON-stat file.

To explore the project:

1. Clone or download this repository.
2. Import the notebook into your Databricks workspace.
3. Download the source dataset from the official CSO API.
4. Upload the JSON-stat file to the configured Unity Catalog volume.
5. Execute the notebook cells sequentially.

The notebook contains the ingestion, transformation, analytical modeling, and data quality validation workflows.

## Project Status

- [x] Source data exploration.
- [x] Bronze layer implementation.
- [x] Silver layer implementation.
- [x] Gold analytical tables.
- [x] Initial data quality validation.
- [x] Architecture documentation.
- [ ] Modular Python implementation.
- [ ] Independent Pytest implementation.
- [ ] Final documentation and project refinement.

## Author

**Brenda Espinosa Barros**

Data Analyst | Aspiring Data Engineer

[GitHub](https://github.com/brendaespinosaa)
