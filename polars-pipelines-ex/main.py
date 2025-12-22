import polars as pl
from datetime import datetime

def scan_filter_select():
    q = (
        pl.scan_parquet("sessions.parquet")
        .filter(pl.col("country") == "IT")
        .select(["user_id", "country", "start_ts", "end_ts"])
    )

    df = q.collect()
    print(df)
    

def one_pass_transformation():
    q = (
        pl.scan_parquet("sessions.parquet")
        .with_columns([
            (pl.col("end_ts") - pl.col("start_ts")).alias("duration_s"),
            pl.col("country").str.to_uppercase().alias("country_u"),
        ])
        .with_columns([
            pl.when(pl.col("duration_s") > 300).then(pl.lit("long")).otherwise(pl.lit("short")).alias("bucket"),
        ])
        .select(["user_id", "duration_s", "bucket", "country_u"])
    )

    df = q.collect()
    print(df)
    # df.write_parquet("processed_sessions.parquet")


def time_series_resampling():    
    q = (
        pl.scan_parquet("ticks.parquet")
        .group_by_dynamic("ts", every="1d")
        .agg(pl.col("price").mean().alias("avg_price"))
    )
    df = q.collect()
    print(df)

def rolling_metrics():
    q = (
        pl.scan_parquet("ticks.parquet")
        .sort("ts")
        .with_columns([
            pl.col("price").rolling_mean(window_size=10).alias("price_rolling_mean_10"),
            pl.col("price").rolling_std(window_size=10).alias("price_rolling_std_10"),
        ])
    )
    df = q.collect()
    print(df)


def streaming_joins():
    events = pl.scan_parquet("events.parquet").select(["user_id", "ts", "event"])
    users = pl.scan_parquet("users.parquet").select(["user_id", "plan"])

    q = (
        events.filter(pl.col("event") == "purchase")
        .join(users, on="user_id", how="left")
    )

    # Use engine="streaming" to reduce memory pressure on complex pipelines
    # Streaming execution processes data in batches rather than loading everything into memory
    # This is crucial for large datasets that don't fit in RAM
    df = q.collect(engine="streaming")

    print(df)
    print(f"\nNote: Query executed with streaming engine for memory efficiency")


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

    df = q.collect()
    print(df)

def subplan_reuse_example():
    lf = pl.scan_parquet("events.parquet")

    q = (
        lf.filter(pl.col("event") == "login")
        .select(["user_id", "ts", "amount"])
        .group_by("user_id")
        .agg(pl.len().alias("login_count"))
    )
    
    print(q.explain())
    
    df = q.collect()
    print(df)


def explode_analyze_example():

    # q = (
    #     pl.scan_parquet("events_nested.parquet")
    #     .explode("tags")
    #     .group_by("tags")
    #     .agg(pl.len().alias("count"))
    #     .sort("count", descending=True)
    # )   


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
    
    print(q.explain())
    
    df = q.collect()
    print(df)


def main():
    scan_filter_select()
    one_pass_transformation()
    time_series_resampling()
    rolling_metrics()
    streaming_joins()
    group_by_aggregations_using_multiple_metrics()
    subplan_reuse_example()
    explode_analyze_example()


if __name__ == "__main__":
    main()