from statistics import mean

import polars as pl
from polars.functions.lazy import n_unique


def eager_and_filters(df: pl.DataFrame) -> pl.DataFrame:

    filtered = df.filter(
        (pl.col("country") == "USA") & (pl.col("revenue") > 1000)
    ).select(["customer_id", "country", "revenue"])

    print(filtered)
    return filtered


def add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
    result = df.with_columns(
        [
            (pl.col("revenue") - pl.col("cost")).alias("profit"),
            (pl.col("revenue") / pl.col("cost")).alias("margin_ratio"),
        ]
    )

    print(result)

    return result


def group_by_and_agg(df: pl.DataFrame) -> pl.DataFrame:
    summary = (
        df.group_by("country")
        .agg(
            [
                pl.col("revenue").sum().alias("total_revenue"),
                pl.col("customer_id").n_unique().alias("unique_customers"),
                pl.col("profit").mean().alias("average_profit"),
            ]
        )
        .sort("total_revenue", descending=True)
    )

    print(summary)
    return summary


def main_eager():
    df = pl.read_csv("data/sales.csv")
    # eager_and_filters(df)
    df_w_profit = add_derived_columns(df)
    group_by_and_agg(df_w_profit)


def main_lazy():
    lazy_df = (
        pl.scan_csv("data/sales.csv")
        .filter(pl.col("revenue") > 1000)
        .select(["country", "revenue", "cost"])
        .with_columns([(pl.col("revenue") - pl.col("cost")).alias("profit")])
        .group_by("country")
        .agg(
            [
                pl.col("profit").mean().alias("total_profit"),
                pl.col("revenue").sum().alias("avg_revenue"),
            ]
        )
        .sort("total_profit", descending=True)
    )

    result = lazy_df.collect()
    print(result)


def window_function():
    df = pl.scan_csv("data/sales.csv")
    ranked = df.with_columns(
        [
            pl.col("revenue")
            .rank("dense", descending=True)
            .over("country")
            .alias("country_revenue_rank")
        ]
    )
    # .sort("country_revenue_rank")

    print(ranked.collect())


if __name__ == "__main__":
    # main_eager()
    # main_lazy()
    window_function()
