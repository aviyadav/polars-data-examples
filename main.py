import polars as pl
from utils import time_it

@time_it
def csv2pq():
    df = pl.read_csv(f"data/events_2024.csv")
    result = (
        df
        .filter(pl.col("country") == "DE")
        .group_by("event_date")
        .agg(pl.col("revenue").sum().alias("total_revenue"))
    )

    result.write_parquet(f"output/revenue_DE.parquet")

@time_it
def csv2pq_lz():
    lf = pl.scan_csv(f"data/events_2024.csv")
    result = (
        lf
        .filter(pl.col("country") == "US")
        .group_by("event_date")
        .agg(pl.col("revenue").sum().alias("total_revenue"))
    )

    result.collect().write_parquet(f"output/revenue_US.parquet")

@time_it
def pushdown_prj_sel():
    lf = pl.scan_parquet(f"output/all_events.parquet")
    selected = (
        lf
        .select([
            pl.col("user_id"),
            pl.col("country"),
            pl.col("revenue"),
            pl.col("event_date"),
        ])
        .filter(pl.col("country") == "CA")
        .group_by("user_id")
        .agg(pl.col("revenue").sum().alias("total_revenue"))
    )

    df = selected.collect()
    print(df)

@time_it
def pushdown_prj_flt():
    lf = pl.scan_parquet(f"output/all_events.parquet")
    filtered = (
        lf
        .filter(
            (pl.col("event_date") >= pl.lit("2024-06-01")) &
            (pl.col("event_date") >= pl.lit("2024-07-01")) &
            (pl.col("country") == "DE")
        )
        .group_by("event_date")
        .agg(pl.col("revenue").sum().alias("total_revenue_DE"))
    )

    df = filtered.collect()
    print(df)

@time_it
def conv2pq(csv_file_name: str):
    df = pl.read_csv(f"data/{csv_file_name}.csv")
    df.write_parquet(f"output/{csv_file_name}.parquet")

@time_it
def events2pq():
    df = pl.read_csv(f"data/events_*.csv")
    df.write_parquet(f"output/all_events.parquet")

def compute_net(row):
    return (row["gross_revenue"] or 0) - (row["tax"] or 0) - (row["discount"] or 0)

@time_it
def nt_native_expr_eg():
    lf = pl.scan_parquet(f"output/orders.parquet")
    lf = lf.with_columns(
        pl.struct(["gross_revenue", "tax", "discount"])
            .map_elements(compute_net)
            .alias("net_revenue")
    )

    df = lf.collect()
    print(df)

@time_it
def native_expr_eg():
    lf = pl.scan_parquet(f"output/orders.parquet")
    lf = lf.with_columns(
        (
            pl.col("gross_revenue").fill_null(0) 
            - pl.col("tax").fill_null(0)
            - pl.col("discount").fill_null(0)
        ).alias("net_revenue")
    )

    df = lf.collect()
    print(df)

if __name__ == "__main__":
    csv2pq()
    csv2pq_lz()
    events2pq()
    pushdown_prj_sel()
    pushdown_prj_flt()
    conv2pq("orders")
    nt_native_expr_eg()
    native_expr_eg()