
[![CI Pipeline](https://github.com/brendaespinosaa/dublin-rental-market-lakehouse/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/brendaespinosaa/dublin-rental-market-lakehouse/actions/workflows/tests.yml)


# Dublin Rental Market Lakehouse

An end-to-end **Data Engineering project** built with Databricks, Apache Spark, PySpark, Delta Lake, and the Medallion Architecture.

This project transforms official Irish rental market data from the Central Statistics Office (CSO) into structured, validated, and analytics-ready datasets. It demonstrates a modular data pipeline covering ingestion, distributed processing, data transformation, quality validation, and analytical data modeling.

The primary focus is the historical rental market in Dublin, Ireland.

## Project Overview

The objective is to design and implement a scalable data processing pipeline following modern data engineering practices.

Key capabilities include:

- Ingestion of official statistical data through the CSO API.
- JSON-stat extraction and dimensional metadata decoding.
- Distributed data processing using Apache Spark and PySpark.
- Medallion Architecture implementation (Bronze, Silver, and Gold).
- Delta Lake storage and table management.
- Modular Python-based data engineering components.
- Data quality checks and dataset reconciliation.
- Automated testing with Pytest.

## Architecture

The pipeline follows the **Medallion Architecture**, separating data processing into three logical layers.

### Bronze — Raw Data Ingestion

Responsible for ingesting and structuring the original source data.

- Loads the official CSO Ireland JSON-stat dataset.
- Extracts observations and dimensional metadata.
- Decodes flattened JSON-stat observations into structured records.
- Preserves original source values and missing observations.
- Stores raw observations in Delta Lake.

### Silver — Data Transformation

Responsible for standardizing and validating the ingested data.

- Standardizes dimension labels and data types.
- Converts rental values into decimal format.
- Extracts reporting year and quarter.
- Creates standardized reporting dates.
- Identifies missing and valid rental observations.
- Preserves null values without replacing them with zero.

### Gold — Analytical Data Products

Creates four analytical Delta tables designed for downstream analysis:

| Dataset | Description |
|---|---|
| `gold_dublin_rent_trends` | Quarterly rental trends for Dublin. |
| `gold_dublin_rent_by_property_type` | Average rental values by property type. |
| `gold_dublin_rent_by_bedrooms` | Average rental values by bedroom category. |
| `gold_dublin_rent_by_location` | Rental comparisons across detailed Dublin geographic categories. |

**Geographic consideration:** CSO location categories may overlap. Detailed geographic categories are therefore treated as separate comparisons and must not be summed as if they represented mutually exclusive regions.

## Data Source

**Central Statistics Office (CSO) Ireland**

| Attribute | Description |
|---|---|
| Dataset | RIQ02 — RTB Average Monthly Rent Report |
| Coverage | 2007 Q4 – 2025 Q4 |
| Format | JSON-stat |
| Measurement | Average monthly rent (EUR) |
| Total observations | 1,367,436 |
| Geographic focus | Dublin |

Official source: [CSO Ireland — RIQ02 Dataset](https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/RIQ02/JSON-stat/2.0/en)

## Key Findings

The processed dataset provides insights into Dublin's historical rental market.

### Dataset Profile

| Metric | Result |
|---|---:|
| Source observations | 1,367,436 |
| Valid rental observations | 376,988 |
| Missing observations | 990,448 |
| Valid observations | 27.57% |
| Missing observations | 72.43% |
| Historical coverage | 2007 Q4 – 2025 Q4 |
| Dublin quarterly trend records | 73 |

Missing observations are a characteristic of the original source dataset. They are preserved as null values throughout the pipeline rather than being incorrectly interpreted as zero.

### Dublin Rental Trends

The CSO aggregate category for Dublin shows the following nominal rental changes:

| Period | Average Monthly Rent |
|---|---:|
| 2007 Q4 | €1,312.69 |
| 2025 Q4 | €2,165.02 |

**Nominal growth: 64.93%**

Values are nominal and have not been adjusted for inflation.

### Rental Values by Property Type — 2025 Q4

| Property Type | Average Monthly Rent |
|---|---:|
| Detached house | €3,057.95 |
| Semi-detached house | €2,598.88 |
| Terrace house | €2,432.52 |
| Apartment | €2,154.18 |
| Other flats | €1,444.97 |

## Data Quality & Automated Testing

Data quality is implemented as a dedicated Python module to validate consistency across the Medallion layers.

The validation module includes **10 automated quality checks**:

1. Source-to-Bronze record count reconciliation.
2. Bronze-to-Silver record count reconciliation.
3. Dimensional uniqueness validation.
4. Rental value consistency.
5. Missing value flag consistency.
6. Valid value flag consistency.
7. Reporting period validation.
8. Gold data validity.
9. Gold quarterly trend uniqueness.
10. Silver-to-Gold data reconciliation.

### Test Results

The project includes an automated Pytest suite covering ingestion, transformations, and data quality validation.

<details>
<summary><strong>Latest test execution results — 28 passed</strong></summary>

| Test Module | Passed | Failed |
|---|---:|---:|
| Ingestion | 10 | 0 |
| Quality | 8 | 0 |
| Transformations | 10 | 0 |
| **Total** | **28** | **0** |

**Test success rate: 100%**

Execution environment: Databricks Free Edition, Python 3.12.3, Pytest 8.4.2, and Spark Connect 4.2.0.

Execution time: 84.49 seconds.

</details>

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks Free Edition | Development and execution environment |
| Apache Spark | Distributed data processing |
| PySpark | Data ingestion and transformation |
| Delta Lake | Reliable data storage and table management |
| Python | Modular data engineering implementation |
| SQL | Data exploration and validation |
| Pytest | Automated testing |
| Unity Catalog | Data organization and volume management |
| Git & GitHub | Version control and project documentation |

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
│   ├── 01_dublin_rental_market_lakehouse.ipynb
│   └── 02_run_project_tests
│
├── src/
│   ├── ingestion/
│   │   └── jsonstat_ingestion.py
│   │
│   ├── transformations/
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   └── gold.py
│   │
│   └── quality/
│       └── validation.py
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

The project is developed and executed using Databricks Free Edition.

The source JSON-stat file is stored in a Unity Catalog volume.

**Source file location:**

```text
/Volumes/workspace/dublin_rental_market/raw_files/rtb_average_monthly_rent.json
```

### Running the Databricks Pipeline

1. Connect the repository to a Databricks workspace using Git folders.
2. Ensure the required Unity Catalog catalog, schema, and volume are available.
3. Download the source dataset from the official CSO API.
4. Upload the JSON-stat file to the configured volume.
5. Open `01_dublin_rental_market_lakehouse` and execute the notebook cells sequentially.

The main notebook contains the data ingestion, JSON-stat decoding, Medallion transformations, analytical table creation, and quality validation workflows.

### Running the Automated Tests

The project test suite is located in the `tests/` directory.

The tests were successfully executed in the Databricks environment using the dedicated `02_run_project_tests` notebook and Pytest.

The modular implementation is maintained separately in `src/`, allowing ingestion, transformation, and validation logic to be tested independently.

## Project Status

- [x] Official CSO data source exploration.
- [x] JSON-stat structure analysis and decoding.
- [x] Bronze layer implementation.
- [x] Silver layer implementation.
- [x] Gold analytical datasets.
- [x] Delta Lake table creation.
- [x] Medallion Architecture implementation.
- [x] Modular Python ingestion components.
- [x] Modular Bronze, Silver, and Gold transformations.
- [x] Modular data quality validation.
- [x] Automated Pytest test suite.
- [x] Successful execution of all 28 automated tests.
- [x] Architecture and project documentation.
- [ ] Automated CI pipeline with GitHub Actions.

## Author

**Brenda Espinosa Barros**

Data Analyst | Aspiring Data Engineer

[GitHub Profile](https://github.com/brendaespinosaa)

---

*Built as a portfolio project to demonstrate practical Data Engineering skills, distributed data processing, data quality engineering, and modern data architecture.*