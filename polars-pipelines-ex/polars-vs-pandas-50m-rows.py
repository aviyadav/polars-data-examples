import polars as pl, pandas as pd, time

# Polars Path
t0 = time.time()
pl_res = (
    pl.scan_parquet("events.parquet")
    .select(["user_id", "ts", "amount"])
    .filter(pl.col("ts") >= pl.datetime(2025, 1, 1))
    .group_by("user_id")
    .agg(pl.col("amount").sum().alias("total_amount"))
    .collect(engine="streaming"))

t_polars = time.time() - t0
print(f"Polars Time: {t_polars:.2f} seconds")

# Pandas Path (with PyArrow)
t0 = time.time()
pd_df = pd.read_parquet("events.parquet", columns=["user_id", "ts", "amount"], engine="pyarrow", dtype_backend="pyarrow")
pd_res = (pd_df[pd_df["ts"] >= pd.Timestamp("2025-01-01")]
          .groupby("user_id", sort=False)["amount"].sum().reset_index(name="total_amount"))
t_pandas = time.time() - t0
print(f"Pandas Time (PyArrow): {t_pandas:.2f} seconds")

# Results Comparison
# Polars Time: 1.16 seconds
# Pandas Time: 9.01 seconds
# Pandas Time (PyArrow): 4.06 seconds