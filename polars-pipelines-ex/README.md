# Polars Pipelines Examples

A comprehensive collection of Polars data pipeline examples demonstrating efficient data processing patterns using lazy evaluation, streaming execution, and various transformations.

## Overview

This project showcases best practices for working with Polars, a fast DataFrame library for Python. It includes examples of:
- Lazy query execution with `scan_parquet`
- Filtering and selection operations
- Complex transformations and aggregations
- Time-series analysis and resampling
- Rolling window calculations
- Streaming joins for memory-efficient processing
- Group-by operations with multiple metrics

## Prerequisites

- Python >= 3.14
- uv (package manager)

## Installation

```bash
# Install dependencies
uv sync
```

## Project Structure

```
.
├── main.py                      # Main pipeline examples
├── generate_events_users.py     # Generate events.parquet and users.parquet
├── generate_ticks.py            # Generate ticks.parquet
├── gen-data.py                  # Generate sessions.parquet
├── events.parquet               # Event data (10K rows)
├── users.parquet                # User data (2K rows)
├── sessions.parquet             # Session data (10K rows)
└── ticks.parquet                # Time-series tick data (10K rows)
```

## Generated Datasets

### sessions.parquet
- **Rows**: 10,000
- **Columns**: `user_id`, `start_ts`, `end_ts`, `country` (2-letter code)
- **Description**: User session data with timestamps and country information

### events.parquet
- **Rows**: 10,000
- **Columns**: `user_id`, `ts`, `event`
- **Events**: purchase, view, click, add_to_cart, login, logout, search, signup
- **Description**: User event tracking data

### users.parquet
- **Rows**: 2,000
- **Columns**: `user_id`, `plan`
- **Plans**: free, basic, premium, enterprise
- **Description**: User subscription plan information

### ticks.parquet
- **Rows**: 10,000
- **Columns**: `ts`, `price`
- **Description**: Time-series price tick data with irregular intervals

## Pipeline Examples

### 1. Scan, Filter, and Select
Demonstrates basic lazy query execution with filtering and column selection.

```python
def scan_filter_select():
    q = (
        pl.scan_parquet("sessions.parquet")
        .filter(pl.col("country") == "IT")
        .select(["user_id", "country", "start_ts", "end_ts"])
    )
    df = q.collect()
```

### 2. One-Pass Transformation
Shows how to compute derived columns and conditional logic in a single pass.

```python
def one_pass_transformation():
    q = (
        pl.scan_parquet("sessions.parquet")
        .with_columns([
            (pl.col("end_ts") - pl.col("start_ts")).alias("duration_s"),
            pl.col("country").str.to_uppercase().alias("country_u"),
        ])
        .with_columns([
            pl.when(pl.col("duration_s") > 300)
                .then(pl.lit("long"))
                .otherwise(pl.lit("short"))
                .alias("bucket"),
        ])
        .select(["user_id", "duration_s", "bucket", "country_u"])
    )
```

### 3. Time-Series Resampling
Resamples irregular time-series data into regular intervals (daily aggregation).

```python
def time_series_resampling():    
    q = (
        pl.scan_parquet("ticks.parquet")
        .group_by_dynamic("ts", every="1d")
        .agg(pl.col("price").mean().alias("avg_price"))
    )
```

### 4. Rolling Metrics
Calculates rolling window statistics for time-series analysis.

```python
def rolling_metrics():
    q = (
        pl.scan_parquet("ticks.parquet")
        .sort("ts")
        .with_columns([
            pl.col("price").rolling_mean(window_size=10).alias("price_rolling_mean_10"),
            pl.col("price").rolling_std(window_size=10).alias("price_rolling_std_10"),
        ])
    )
```

### 5. Streaming Joins
Memory-efficient joins using the streaming engine for large datasets.

```python
def streaming_joins():
    events = pl.scan_parquet("events.parquet").select(["user_id", "ts", "event"])
    users = pl.scan_parquet("users.parquet").select(["user_id", "plan"])

    q = (
        events.filter(pl.col("event") == "purchase")
        .join(users, on="user_id", how="left")
    )
    
    # Use streaming engine to reduce memory pressure
    df = q.collect(engine="streaming")
```

### 6. Group-By with Multiple Metrics
Demonstrates complex aggregations with multiple metrics in a single operation.

```python
def group_by_aggregations_using_multiple_metrics():
    q = (
        pl.scan_parquet("events.parquet")
        .group_by("country")
        .agg([
            pl.len().alias("events"),
            pl.col("user_id").n_unique().alias("users"),
            pl.col("amount").sum().alias("revenue"),
            pl.col("amount").quantile(0.95).alias("p95_amount"),
        ])
        .sort("events", descending=True)
    )
```

### 7. Explode and Analyze
Shows how to work with nested/split data using explode.

```python
def explode_analyze_example():
    lf = pl.scan_parquet("events.parquet")

    q = (
        lf.with_columns([
            pl.col("event").str.split("_").alias("event_parts")
        ])
        .explode("event_parts")
        .group_by("event_parts")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
```

## Running the Examples

### Generate all data files
```bash
uv run python gen-data.py
uv run python generate_events_users.py
uv run python generate_ticks.py
```

### Run all pipeline examples
```bash
uv run python main.py
```

### Run individual examples
Edit [main.py](main.py) and comment/uncomment specific functions in the `main()` function.

## Key Concepts

### Lazy Evaluation
- Use `scan_parquet()` instead of `read_parquet()` for lazy execution
- Queries are optimized before execution
- Only necessary data is loaded

### Streaming Engine
- Use `collect(engine="streaming")` for memory-efficient processing
- Processes data in batches rather than loading everything into memory
- Critical for datasets larger than available RAM

### Query Optimization
- Polars automatically optimizes query plans
- Predicate pushdown: filters applied early
- Projection pushdown: only needed columns are read
- Use `.explain()` to see the optimized query plan

## Performance Tips

1. **Use lazy evaluation** (`scan_parquet`) instead of eager (`read_parquet`)
2. **Filter early** in the pipeline to reduce data volume
3. **Select only needed columns** to minimize memory usage
4. **Use streaming engine** for large datasets
5. **Leverage vectorized operations** instead of row-by-row processing
6. **Chain operations** to enable query optimization

## Dependencies

- `polars >= 1.36.1` - Fast DataFrame library
- `pyarrow >= 18.1.0` - Parquet file format support
- `numpy >= 2.2.1` - Numerical computing
- `faker >= 33.1.0` - Generate fake data

## License

MIT

## Resources

- [Polars Documentation](https://docs.pola.rs/)
- [Polars GitHub](https://github.com/pola-rs/polars)
- [Polars User Guide](https://docs.pola.rs/user-guide/)
