import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from functools import partial


def generate_events_chunk(chunk_info):
    """
    Generate a chunk of event data.
    
    Args:
        chunk_info: Tuple of (chunk_id, chunk_size, base_date, seed)
    
    Returns:
        Polars DataFrame with event data
    """
    chunk_id, chunk_size, base_date, seed = chunk_info
    
    # Use unique seed for each chunk
    fake = Faker()
    Faker.seed(seed + chunk_id)
    np.random.seed(seed + chunk_id)
    
    # Generate user IDs (1000 unique users)
    user_ids = np.random.randint(1000, 9999, size=chunk_size)
    
    # Generate timestamps over the last 30 days
    timestamps = [
        base_date - timedelta(
            days=np.random.randint(0, 30),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60),
            seconds=np.random.randint(0, 60)
        )
        for _ in range(chunk_size)
    ]
    
    # Generate event types with realistic distribution
    event_types = ["purchase", "view", "click", "add_to_cart", "login", "logout", "search", "signup"]
    event_weights = [0.05, 0.40, 0.25, 0.10, 0.08, 0.05, 0.05, 0.02]  # Purchase is rare, view is common
    events = np.random.choice(event_types, size=chunk_size, p=event_weights)

    amounts = [3500, 4150, 9100, 5400, 1200, 800, 600, 300]
    
    # Generate 2-letter country codes
    country_codes = [fake.country_code() for _ in range(chunk_size)]

    # Create Polars DataFrame
    df = pl.DataFrame({
        "user_id": user_ids,
        "ts": timestamps,
        "event": events,
        "amount": np.random.choice(amounts, size=chunk_size),
        "country": country_codes,
    })
    
    return df


def generate_events_parquet(num_rows: int = 10000, output_file: str = "events.parquet", num_processes: int = None):
    """
    Generate a parquet file with event data using multiprocessing.
    
    Args:
        num_rows: Number of rows to generate (default: 10000)
        output_file: Output parquet file name (default: events.parquet)
        num_processes: Number of processes to use (default: CPU count)
    """
    if num_processes is None:
        num_processes = cpu_count()
    
    print(f"Generating {num_rows:,} events using {num_processes} processes...")
    
    base_date = datetime.now()
    seed = 42
    
    # Calculate chunk sizes
    chunk_size = num_rows // num_processes
    remainder = num_rows % num_processes
    
    # Create chunk info for each process
    chunk_infos = []
    for i in range(num_processes):
        size = chunk_size + (1 if i < remainder else 0)
        chunk_infos.append((i, size, base_date, seed))
    
    # Generate data in parallel
    with Pool(processes=num_processes) as pool:
        chunks = pool.map(generate_events_chunk, chunk_infos)
    
    # Concatenate all chunks
    print("Combining chunks...")
    df = pl.concat(chunks, how="vertical")
    
    # Sort by timestamp
    print("Sorting by timestamp...")
    df = df.sort("ts")
    
    # Write to parquet
    print("Writing to parquet...")
    df.write_parquet(output_file, use_pyarrow=True)
    
    print(f"✓ Generated {output_file} with {num_rows:,} rows")
    print(f"\nDataFrame schema:")
    print(df.schema)
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nEvent distribution:")
    print(df.group_by("event").agg(pl.count().alias("count")).sort("count", descending=True))
    
    return df


def generate_users_chunk(chunk_info):
    """
    Generate a chunk of user data.
    
    Args:
        chunk_info: Tuple of (start_id, chunk_size, seed)
    
    Returns:
        Polars DataFrame with user data
    """
    start_id, chunk_size, seed = chunk_info
    
    # Use unique seed for each chunk
    np.random.seed(seed + start_id)
    
    # Generate unique user IDs
    user_ids = np.arange(start_id, start_id + chunk_size)
    
    # Generate plan types with realistic distribution
    plan_types = ["free", "basic", "premium", "enterprise"]
    plan_weights = [0.60, 0.25, 0.12, 0.03]  # Most users are free, enterprise is rare
    plans = np.random.choice(plan_types, size=chunk_size, p=plan_weights)
    
    # Create Polars DataFrame
    df = pl.DataFrame({
        "user_id": user_ids,
        "plan": plans,
    })
    
    return df


def generate_users_parquet(num_users: int = 2000, output_file: str = "users.parquet", num_processes: int = None):
    """
    Generate a parquet file with user data using multiprocessing.
    
    Args:
        num_users: Number of users to generate (default: 2000)
        output_file: Output parquet file name (default: users.parquet)
        num_processes: Number of processes to use (default: CPU count)
    """
    if num_processes is None:
        num_processes = cpu_count()
    
    print(f"\nGenerating {num_users:,} users using {num_processes} processes...")
    
    seed = 42
    start_user_id = 1000
    
    # Calculate chunk sizes
    chunk_size = num_users // num_processes
    remainder = num_users % num_processes
    
    # Create chunk info for each process
    chunk_infos = []
    current_id = start_user_id
    for i in range(num_processes):
        size = chunk_size + (1 if i < remainder else 0)
        chunk_infos.append((current_id, size, seed))
        current_id += size
    
    # Generate data in parallel
    with Pool(processes=num_processes) as pool:
        chunks = pool.map(generate_users_chunk, chunk_infos)
    
    # Concatenate all chunks
    print("Combining chunks...")
    df = pl.concat(chunks, how="vertical")
    
    # Write to parquet
    print("Writing to parquet...")
    df.write_parquet(output_file, use_pyarrow=True)
    
    print(f"✓ Generated {output_file} with {num_users:,} rows")
    print(f"\nDataFrame schema:")
    print(df.schema)
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nPlan distribution:")
    print(df.group_by("plan").agg(pl.len().alias("count")).sort("count", descending=True))
    
    return df


if __name__ == "__main__":
    import time
    
    # Measure execution time
    start_time = time.time()
    
    generate_events_parquet(num_rows=50_000_000, output_file="events.parquet")
    generate_users_parquet(num_users=200_000, output_file="users.parquet")
    
    end_time = time.time()
    print(f"\n{'='*60}")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    print(f"{'='*60}")
