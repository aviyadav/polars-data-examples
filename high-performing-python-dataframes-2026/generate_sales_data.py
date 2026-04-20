#!/usr/bin/env python3
"""
Generate sales.csv with random data using multiprocessing, polars, and pyarrow.
"""

import multiprocessing as mp
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

# Configuration
TOTAL_ROWS = 1_000_000
CHUNK_SIZE = 10_000  # Rows per chunk
OUTPUT_FILE = Path(__file__).parent / "data" / "sales.csv"

# Random data pools
COUNTRIES = [
    "USA",
    "Canada",
    "UK",
    "Germany",
    "France",
    "Spain",
    "Italy",
    "Japan",
    "Australia",
    "Brazil",
    "India",
    "China",
    "Mexico",
    "Netherlands",
    "Sweden",
]


def generate_order_id(length: int = 10) -> str:
    """Generate a random order ID."""
    chars = string.ascii_uppercase + string.digits
    return "ORD-" + "".join(random.choices(chars, k=length))


def generate_date_range(start_year: int = 2022, end_year: int = 2024) -> datetime:
    """Generate a random date within the range."""
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    return start_date + timedelta(days=random_days)


def generate_chunk(chunk_id: int, chunk_size: int) -> pl.DataFrame:
    """Generate a chunk of sales data."""
    data = {
        "customer_id": [
            f"CUST-{random.randint(1, 5000):05d}" for _ in range(chunk_size)
        ],
        "country": [random.choice(COUNTRIES) for _ in range(chunk_size)],
        "revenue": [round(random.uniform(10.0, 10000.0), 2) for _ in range(chunk_size)],
        "cost": [round(random.uniform(5.0, 5000.0), 2) for _ in range(chunk_size)],
        "order_id": [generate_order_id() for _ in range(chunk_size)],
        "order_date": [generate_date_range() for _ in range(chunk_size)],
    }
    return pl.DataFrame(data)


def main():
    """Main function to generate sales data."""
    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Calculate number of chunks
    num_chunks = (TOTAL_ROWS + CHUNK_SIZE - 1) // CHUNK_SIZE

    print(f"Generating {TOTAL_ROWS:,} rows in {num_chunks} chunks...")
    print(f"Using {mp.cpu_count()} CPU cores")

    # Use multiprocessing to generate chunks
    with mp.Pool() as pool:
        # Prepare chunk arguments - each chunk gets its size
        chunk_args = []
        for i in range(num_chunks):
            # Last chunk might be smaller
            size = min(CHUNK_SIZE, TOTAL_ROWS - i * CHUNK_SIZE)
            chunk_args.append((i, size))

        # Generate chunks in parallel
        chunks = pool.starmap(generate_chunk, chunk_args)

    # Concatenate all chunks into a single DataFrame
    print("Concatenating chunks...")
    df = pl.concat(chunks)

    # Write to CSV
    print(f"Writing to {OUTPUT_FILE}...")
    df.write_csv(OUTPUT_FILE)

    print(f"Done! Generated {len(df):,} rows")
    print(f"File size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()
