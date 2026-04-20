# High Performing Python Dataframes 2026

A project demonstrating high-performance data manipulation using **Polars** and **PyArrow** for large-scale data processing.

## Overview

This project showcases efficient data processing techniques with Polars, a fast DataFrame library written in Rust. It includes:

- **Eager vs Lazy evaluation**: Compare different execution modes
- **Filtering and selections**: Efficient data filtering
- **Derived columns**: Adding computed columns
- **Group-by aggregations**: Summarizing data by groups
- **Window functions**: Ranking and partitioning operations
- **Multiprocessing data generation**: Fast CSV generation using parallel processing

## Prerequisites

- Python 3.14+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Installation

```bash
# Clone the repository
cd high-performing-python-dataframes-2026

# Install dependencies using uv
uv sync

# Or using pip
pip install -r requirements.txt
```

## Project Structure

```
.
├── data/
│   └── sales.csv          # Generated sales data (100,000 rows)
├── generate_sales_data.py # Script to generate random sales data
├── main.py                # Main data processing demonstrations
├── pyproject.toml         # Project configuration and dependencies
└── README.md
```

## Usage

### 1. Generate Sales Data

First, generate the `sales.csv` file with 100,000 rows of random sales data:

```bash
python generate_sales_data.py
```

This will create `data/sales.csv` with the following columns:

| Column       | Description                          |
|--------------|--------------------------------------|
| customer_id  | Unique customer identifier           |
| country      | Customer's country (15 countries)    |
| revenue      | Order revenue ($10 - $10,000)        |
| cost         | Order cost ($5 - $5,000)             |
| order_id     | Unique order identifier              |
| order_date   | Order date (2022-2024)               |

The script uses multiprocessing to generate data in chunks for optimal performance.

### 2. Run Data Processing Examples

```bash
python main.py
```

The `main.py` file contains several demonstration functions:

#### Eager Execution (`main_eager()`)

- Loads data into memory immediately
- Adds derived columns (profit, margin ratio)
- Performs group-by aggregation by country

#### Lazy Execution (`main_lazy()`)

- Builds an optimized query plan
- Executes only when `collect()` is called
- Filters, selects, computes, and aggregates in one pipeline
- Sorts results by total profit

#### Window Functions (`window_function()`)

- Uses Polars lazy API with `scan_csv()`
- Computes dense ranking of revenue within each country
- Sorts final results by the computed rank

## Key Features

### Polars LazyFrame

```python
lazy_df = (
    pl.scan_csv("data/sales.csv")
    .filter(pl.col("revenue") > 1000)
    .with_columns([(pl.col("revenue") - pl.col("cost")).alias("profit")])
    .group_by("country")
    .agg([
        pl.col("profit").mean().alias("total_profit"),
        pl.col("revenue").sum().alias("avg_revenue"),
    ])
    .sort("total_profit", descending=True)
)
result = lazy_df.collect()
```

### Window Functions with Partitioning

```python
ranked = (
    pl.scan_csv("data/sales.csv")
    .with_columns([
        pl.col("revenue")
        .rank("dense", descending=True)
        .over("country")
        .alias("country_revenue_rank")
    ])
    .sort("country_revenue_rank")
)
```

## Dependencies

| Package  | Version   | Purpose                    |
|----------|-----------|----------------------------|
| polars   | >= 1.40.0 | DataFrame operations       |
| pyarrow  | >= 23.0.1 | Arrow format support       |

## Performance Tips

1. **Use Lazy API**: `scan_csv()` + `collect()` allows query optimization
2. **Chunked Processing**: Process large files in chunks to reduce memory usage
3. **Multiprocessing**: Use `multiprocessing.Pool` for parallel data generation
4. **Avoid Pandas**: Polars is significantly faster for most operations
