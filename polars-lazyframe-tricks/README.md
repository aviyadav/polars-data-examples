# polars-lazyframe-tricks

This project demonstrates advanced usage of [Polars](https://www.pola.rs/) DataFrames and LazyFrames for efficient data processing in Python. It includes examples of data generation, eager vs. lazy execution, predicate and projection pushdown, and custom expressions.

## Project Structure

- `main.py` — Main script with Polars DataFrame and LazyFrame tricks, including:
  - Eager and lazy CSV to Parquet conversion
  - Predicate and projection pushdown examples
  - Native and custom expression usage
- `generate_data.py` — Script to generate synthetic event and order data as CSV files in the `data/` directory.
- `utils.py` — Utility functions, including a timing decorator for benchmarking.
- `data/` — Folder for generated CSV data files (created by `generate_data.py`).
- `output/` — Folder for generated Parquet files and results.
- `pyproject.toml` — Project dependencies and metadata.

## Setup

1. **Install dependencies** (Python 3.13+ required):
	```sh
	pip install polars>=1.35.2
	```

2. **Generate data:**
	```sh
	python generate_data.py
	```
	This will create event and order CSV files in the `data/` directory.

3. **Run main examples:**
	```sh
	python main.py
	```
	This will execute all Polars tricks and print timing/results to the console.

## Example Functions in `main.py`

- `csv2pq()` — Eagerly reads a CSV, filters, groups, and writes to Parquet.
- `csv2pq_lz()` — Lazily reads a CSV, filters, groups, and writes to Parquet.
- `pushdown_prj_sel()` — Demonstrates projection and predicate pushdown with LazyFrame.
- `pushdown_prj_flt()` — Shows filtering and aggregation with pushdown optimizations.
- `conv2pq(csv_file_name)` — Converts a CSV file to Parquet.
- `events2pq()` — Merges all event CSVs into a single Parquet file.
- `nt_native_expr_eg()` — Adds a custom column using a Python function with `map_elements`.
- `native_expr_eg()` — Adds a computed column using native Polars expressions.

## Notes

- All scripts print execution time for each function using the `@time_it` decorator.
- Data and output folders are created automatically if missing.
- See code comments for further details and customization options.