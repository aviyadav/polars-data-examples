import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

def generate_events_parquet(num_rows: int = 10000, output_file: str = "events.parquet"):
    """
    Generate a parquet file with event data.
    
    Args:
        num_rows: Number of rows to generate (default: 10000)
        output_file: Output parquet file name (default: events.parquet)
    """
    fake = Faker()
    np.random.seed(42)
    
    # Generate user IDs (1000 unique users)
    user_ids = np.random.randint(1000, 9999, size=num_rows)
    
    # Generate timestamps over the last 30 days
    base_date = datetime.now()
    timestamps = [
        base_date - timedelta(
            days=np.random.randint(0, 30),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60),
            seconds=np.random.randint(0, 60)
        )
        for _ in range(num_rows)
    ]
    
    # Generate event types with realistic distribution
    event_types = ["purchase", "view", "click", "add_to_cart", "login", "logout", "search", "signup"]
    event_weights = [0.05, 0.40, 0.25, 0.10, 0.08, 0.05, 0.05, 0.02]  # Purchase is rare, view is common
    events = np.random.choice(event_types, size=num_rows, p=event_weights)

    amounts = [3500, 4150, 9100, 5400, 1200, 800, 600, 300]
    
    # Generate 2-letter country codes
    country_codes = [fake.country_code() for _ in range(num_rows)]

    # Create Polars DataFrame
    df = pl.DataFrame({
        "user_id": user_ids,
        "ts": timestamps,
        "event": events,
        "amount": np.random.choice(amounts, size=num_rows),
        "country": country_codes,
    })
    
    # Sort by timestamp
    df = df.sort("ts")
    
    # Write to parquet
    df.write_parquet(output_file, use_pyarrow=True)
    
    print(f"✓ Generated {output_file} with {num_rows} rows")
    print(f"\nDataFrame schema:")
    print(df.schema)
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nEvent distribution:")
    print(df.group_by("event").agg(pl.count().alias("count")).sort("count", descending=True))
    
    return df


def generate_users_parquet(num_users: int = 2000, output_file: str = "users.parquet"):
    """
    Generate a parquet file with user data.
    
    Args:
        num_users: Number of users to generate (default: 2000)
        output_file: Output parquet file name (default: users.parquet)
    """
    np.random.seed(42)
    
    # Generate unique user IDs
    user_ids = np.arange(1000, 1000 + num_users)
    
    # Generate plan types with realistic distribution
    plan_types = ["free", "basic", "premium", "enterprise"]
    plan_weights = [0.60, 0.25, 0.12, 0.03]  # Most users are free, enterprise is rare
    plans = np.random.choice(plan_types, size=num_users, p=plan_weights)
    
    # Create Polars DataFrame
    df = pl.DataFrame({
        "user_id": user_ids,
        "plan": plans,
    })
    
    # Write to parquet
    df.write_parquet(output_file, use_pyarrow=True)
    
    print(f"\n✓ Generated {output_file} with {num_users} rows")
    print(f"\nDataFrame schema:")
    print(df.schema)
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nPlan distribution:")
    print(df.group_by("plan").agg(pl.count().alias("count")).sort("count", descending=True))
    
    return df


if __name__ == "__main__":
    generate_events_parquet(num_rows=10000, output_file="events.parquet")
    generate_users_parquet(num_users=2000, output_file="users.parquet")
