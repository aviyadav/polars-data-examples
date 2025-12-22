import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

def generate_sessions_parquet(num_rows: int = 10000, output_file: str = "sessions.parquet"):
    """
    Generate a parquet file with session data using polars, pyarrow, numpy and faker.
    
    Args:
        num_rows: Number of rows to generate (default: 10000)
        output_file: Output parquet file name (default: sessions.parquet)
    """
    fake = Faker()
    np.random.seed(42)
    
    # Generate user IDs (1000 unique users)
    user_ids = np.random.randint(1000, 9999, size=num_rows)
    
    # Generate start timestamps (random timestamps over the last 30 days)
    base_date = datetime.now()
    start_timestamps = [
        base_date - timedelta(
            days=np.random.randint(0, 30),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60),
            seconds=np.random.randint(0, 60)
        )
        for _ in range(num_rows)
    ]
    
    # Generate end timestamps (start_ts + random session duration between 1 minute and 4 hours)
    end_timestamps = [
        start_ts + timedelta(
            minutes=np.random.randint(1, 240)
        )
        for start_ts in start_timestamps
    ]
    
    # Generate 2-letter country codes
    country_codes = [fake.country_code() for _ in range(num_rows)]
    
    # Create Polars DataFrame
    df = pl.DataFrame({
        "user_id": user_ids,
        "start_ts": start_timestamps,
        "end_ts": end_timestamps,
        "country": country_codes,
    })
    
    # Write to parquet using pyarrow engine
    df.write_parquet(output_file, use_pyarrow=True)
    
    print(f"✓ Generated {output_file} with {num_rows} rows")
    print(f"\nDataFrame schema:")
    print(df.schema)
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nDataFrame shape: {df.shape}")
    
    return df


def main():
    generate_sessions_parquet(num_rows=10000, output_file="sessions.parquet")


if __name__ == "__main__":
    main()
