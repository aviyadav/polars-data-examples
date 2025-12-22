# polars expressions that retire loops


#### Expressions vs. Loops

```
import polars as pl

df = (
    pl.read_csv("events.csv")
      .with_columns(
          pl.col("price") * pl.col("quantity")
          .alias("revenue")
      )
      .groupby("country")
      .agg(pl.col("revenue").sum())
)
```


####  Row-Wise Calculations

```
# old mental model
for row in rows:
    row["revenue"] = row["price"] * row["quantity"]

# in polars [NEW]
df = df.with_columns(
    (pl.col("price") * pl.col("quantity")).alias("revenue")
)


df = df.with_columns(
    (pl.col("price") * pl.col("quantity")).alias("revenue"),
    pl.col("revenue").round(2).alias("revenue_rounded"),
    (pl.col("revenue") > 1000).alias("is_big_deal"),
)
```

#### Conditional Logic

```
# imperative style
for row in rows:
    if row["country"] == "US":
        row["region"] = "NA"
    elif row["country"] in {"DE", "FR", "ES"}:
        row["region"] = "EU"
    else:
        row["region"] = "Other"

# in polars

df = df.with_columns(
    pl.when(pl.col("country") == "US").then("NA")
      .when(pl.col("country").is_in(["DE", "FR", "ES"])).then("EU")
      .otherwise("Other")
      .alias("region")
)

# or nested
df = df.with_columns(
    pl.when(pl.col("revenue") > 1_000)
      .then(pl.lit("VIP"))
      .otherwise("Standard")
      .alias("segment")
)


vip_condition = (pl.col("country") == "US") & (pl.col("revenue") > 1_000)

df = df.with_columns(
    pl.when(vip_condition).then("US-VIP").otherwise("Other").alias("segment")
)

```

#### Complex Grouped Aggregations

```
# pseudo-code
stats = {}
for row in rows:
    key = row["customer_id"]
    stats.setdefault(key, {"total": 0, "count": 0})
    stats[key]["total"] += row["amount"]
    stats[key]["count"] += 1

for key, agg in stats.items():
    agg["avg"] = agg["total"] / agg["count"]


# polars
summary = (
    df.groupby("customer_id")
      .agg(
          pl.col("amount").sum().alias("total_amount"),
          pl.col("amount").mean().alias("avg_amount"),
          pl.count().alias("num_orders"),
          pl.col("timestamp").max().alias("last_order_at"),
      )
)
```


#### windows function

```
# rolling customer-level cumulative sum
totals = {}
for row in rows:
    cust = row["customer_id"]
    totals.setdefault(cust, 0)
    totals[cust] += row["amount"]
    row["cust_running_total"] = totals[cust]


# polars

df = df.sort(["customer_id", "timestamp"]).with_columns(
    pl.col("amount")
      .cumsum()
      .over("customer_id")
      .alias("cust_running_total")
)

# or

df = df.with_columns(
    pl.col("amount").mean().over("customer_id").alias("cust_avg_amount"),
    pl.col("amount").max().over("customer_id").alias("cust_max_amount"),
)
```

#### Custom Logic Without Leaving Expressions

```
lf = df.lazy()

def add_custom_flag(batch: pl.DataFrame) -> pl.DataFrame:
    # regular Polars logic inside
    return batch.with_columns(
        (pl.col("amount") > 500).alias("is_high_value")
    )

lf = lf.map_batches(add_custom_flag)
result = lf.collect()

# row level logic

df = df.with_columns(
    pl.struct(["a", "b"]).apply(
        lambda row: row["a"] ** 2 + row["b"] ** 2
    ).alias("hypot")
)

```


#### Real-World Mini Case
> Loop through events
> Compute event_date from timestamp
> Tag users as “new” or “returning”
> Keep customer-level running totals
> Group by date and country for a daily dashboard

```
import polars as pl

df = pl.read_parquet("events.parquet")

clean = (
    df
    .with_columns(
        pl.col("timestamp").dt.date().alias("event_date"),
        (pl.col("event_type") == "signup").alias("is_signup"),
    )
    .with_columns(
        pl.when(pl.col("is_signup"))
          .then("new")
          .otherwise("returning")
          .alias("user_status")
    )
    .sort(["user_id", "timestamp"])
    .with_columns(
        pl.col("revenue").fill_null(0),
        pl.col("revenue").cumsum().over("user_id").alias("user_ltv"),
    )
    .groupby(["event_date", "country"])
    .agg(
        pl.count().alias("num_events"),
        pl.col("user_id").n_unique().alias("num_users"),
        pl.col("revenue").sum().alias("total_revenue"),
    )
)
```
