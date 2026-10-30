# Dublin Rental Market Lakehouse

An end-to-end data engineering project that processes official Irish rental market data using **Databricks, Apache Spark, PySpark, Delta Lake, and the Medallion Architecture**.

The project transforms raw statistical data from Ireland's Central Statistics Office (CSO) into structured, validated, and analytical datasets, focusing on rental market trends in Dublin.

## Project Overview

The objective is to develop a modular and scalable data processing pipeline that demonstrates key data engineering concepts, including:

- Data ingestion from an official statistical API.
- JSON-stat data extraction and dimensional metadata handling.
- Distributed data processing with PySpark.
- Medallion Architecture implementation (Bronze, Silver, and Gold).
- Delta Lake storage and table management.
- Data quality validation and reconciliation.
- Modular Python development.
- Automated testing with Pytest.

## Architecture

The project follows the **Medallion Architecture**, organizing data processing into three layers.

### Bronze — Raw Data Ingestion

- Ingests the official CSO Ireland JSON-stat dataset.
- Extracts observations and dimensional metadata.
- Decodes flattened JSON-stat observations into structured records.
- Preserves original source values and missing observations.
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
| `gold_dublin_rent_by_location` | Rental comparisons across detailed Dublin geographic categories. |

**Geographic consideration:** CSO location categories may overlap. Therefore, detailed geographic categories are treated as separate comparisons and must not be summed as if they were mutually exclusive regions.

## Data Source

**Central Statistics Office (CSO) Ireland**

- Dataset: RIQ02 — RTB Average Monthly Rent Report.
- Coverage: 2007 Q4 to 2025 Q4.
- Format: JSON-stat.
- Measurement: Average monthly rent in EUR.
- Total observations: 1,367,436.

Official API:

[CSO Ireland — RIQ02 Dataset](https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/RIQ02/JSON-stat/2.0/en)

## Key Findings

The processed dataset provides insights into Dublin's historical rental market.

| Metric | Result |
|---|---:|
| Source observations | 1,367,436 |
| Valid rental observations | 376,988 |
| Missing observations | 990,448 |
| Valid observations | 27.57% |
| Missing observations | 72.43% |
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

The project implements automated data quality validation to ensure consistency across the Medallion layers.

The modular validation module includes **10 quality checks**:

1. Source-to-Bronze record count reconciliation.
2. Bronze-to-Silver record count reconciliation.
3. Dimensional uniqueness.
4. Rental value consistency.
5. Missing value flag consistency.
6. Valid value flag consistency.
7. Reporting period validation.
8. Gold data validity.
9. Gold quarterly trend uniqueness.
10. Silver-to-Gold reconciliation.

The project also includes an independent Pytest suite covering ingestion, transformations, and data quality validation.

**Test execution and final results are pending.**

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks Free Edition | Development and execution environment |
| Apache Spark | Distributed data processing |
| PySpark | Data ingestion and transformation |
| Delta Lake | Data storage and table management |
| Python | Modular data engineering logic |
| SQL | Data exploration and validation |
| Pytest | Automated testing |
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
│   │   ├── jsonstat_ingestion.py
│   │   └── .gitkeep
│   │
│   ├── transformations/
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   ├── gold.py
│   │   └── .gitkeep
│   │
│   └── quality/
│       ├── validation.py
│       └── .gitkeep
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_ingestion.py
│   ├── test_transformations.py
│   └── test_quality.py
│
├── .gitignore
├── requirements.txt
└── README.md
```

## Execution Environment

The project is developed using Databricks Free Edition.

The current implementation uses a Unity Catalog volume to store the source JSON-stat file.

**Source file location:**

`/Volumes/workspace/dublin_rental_market/raw_files/rtb_average_monthly_rent.json`

To explore the Databricks implementation:

1. Clone or download this repository.
2. Import the notebook into your Databricks workspace.
3. Download the source dataset from the official CSO API.
4. Upload the JSON-stat file to the configured Unity Catalog volume.
5. Execute the notebook cells sequentially.

The notebook contains the original ingestion, transformation, analytical modeling, and data quality workflows.

The modular Python implementation and automated tests are maintained separately in `src/` and `tests/`.

## Project Status

- [x] Official source data exploration.
- [x] JSON-stat structure analysis.
- [x] Bronze layer implementation.
- [x] Silver layer implementation.
- [x] Gold analytical datasets.
- [x] Initial data quality validation.
- [x] Medallion Architecture documentation.
- [x] Modular Python ingestion implementation.
- [x] Modular Bronze, Silver, and Gold transformations.
- [x] Modular data quality validation.
- [x] Pytest test suite implementation.
- [x] Python and Java local environment preparation.
- [ ] Local dependency installation and environment configuration.
- [ ] Automated test execution and issue resolution.
- [ ] Integration of modular Python code with the Databricks notebook.
- [ ] Final documentation review.

## Author

**Brenda Espinosa Barros**

Data Analyst | Aspiring Data Engineer

[GitHub](https://github.com/brendaespinosaa)
