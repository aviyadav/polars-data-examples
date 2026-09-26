# Finance & Supply Chain Data Analytics

A hands-on Python project focused on learning and applying Polars for data analysis, transformation, and reporting using sample finance and supply-chain datasets.

## Project Overview

This workspace contains a Polars-based data exploration notebook and supporting sample data for:

- Financial ledger analysis
- Supply chain and operational data modeling
- DataFrame creation, filtering, aggregation, and export
- Practical examples of data cleaning and analytical workflows

The main notebook is:

- `Polars dataframe complete user guide.ipynb`

It demonstrates data modeling, DataFrame expressions, grouping, joins, transformations, and CSV/parquet handling using Polars.

## Repository Structure

```text
.
├── Polars dataframe complete user guide.ipynb
├── README.md
├── requirements.txt
├── data/
│   ├── ledger.csv
│   └── ledger.json
└── .ipynb_checkpoints/
```

## Data Included

### Finance dataset
The sample finance dataset contains ledger-style records with fields such as:

- `LEDGER`
- `ORG`
- `FISCAL_YEAR`
- `PERIOD`
- `ACCOUNT`
- `DEPT`
- `LOCATION`
- `POSTED_TOTAL`

The dataset is available in:

- `data/ledger.csv`
- `data/ledger.json`

### Supply chain dataset
The notebook also creates supporting reference tables for:

- locations
- departments
- accounts
- product categories
- products
- customers
- orders
- invoices

These examples model how operational data can be organized and analyzed in a DataFrame workflow.

## Tech Stack

- Python
- Polars
- Jupyter Notebook
- NumPy
- CSV/JSON data samples

## Setup

1. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Launch Jupyter Notebook:

```bash
jupyter notebook
```

4. Open the notebook `Polars dataframe complete user guide.ipynb` and run the cells.

## Typical Analytics Covered

- Constructing DataFrames from dictionaries and collections
- Sampling and exploring data
- Filtering rows by conditions
- Transforming columns and casting data types
- Grouping and aggregating totals by year, ledger, or category
- Working with categorical data
- Saving results to CSV and Parquet files
- Loading data from flat files and structured JSON

## Example Use Cases

- Budget vs actual financial comparisons
- Department and location-level performance review
- Supply chain operation summarization
- DataFrame-based analytics and reporting practice with Polars

## Notes

This project is designed as a learning and demonstration repository for Polars and dataframe-based analytics. It is not connected to a production database or enterprise ERP system.

## License

This project is provided for educational use.
