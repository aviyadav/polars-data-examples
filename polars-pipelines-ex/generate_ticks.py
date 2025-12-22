import polars as pl
import numpy as np
from datetime import datetime, timedelta

def generate_ticks_parquet(num_rows: int = 10000, output_file: str = "ticks.parquet"):
    """
    Generate a parquet file with time-series tick data.
    
    Args:
        num_rows: Number of rows to generate (default: 10000)
        output_file: Output parquet file name (default: ticks.parquet)
    """
    np.random.seed(42)
    
    # Generate timestamps over 30 days with irregular intervals
    base_date = datetime.now() - timedelta(days=30)
    timestamps = []
    current_time = base_date
    
    for _ in range(num_rows):
        # Add random seconds between ticks (1 second to 5 minutes)
        current_time += timedelta(seconds=np.random.randint(1, 300))
        timestamps.append(current_time)
    
    # Generate price data with some realistic variation
    base_price = 100.0
    prices = [base_price]
    
    for _ in range(num_rows - 1):
        # Random walk with small changes
        change = np.random.normal(0, 0.5)  # Mean 0, std 0.5
        new_price = prices[-1] + change
        prices.append(max(50.0, min(150.0, new_price)))  # Keep between 50 and 150
    
    # Create Polars DataFrame
    df = pl.DataFrame({
        "ts": timestamps,
        "price": prices,
    })
    
    # Write to parquet using pyarrow engine
    df.write_parquet(output_file, use_pyarrow=True)
    
    print(f"✓ Generated {output_file} with {num_rows} rows")
    print(f"\nDataFrame schema:")
    print(df.schema)
    print(f"\nFirst 5 rows:")
    print(df.head())
    print(f"\nLast 5 rows:")
    print(df.tail())
    print(f"\nDataFrame shape: {df.shape}")
    print(f"\nPrice statistics:")
    print(df.select("price").describe())
    
    return df


if __name__ == "__main__":
    generate_ticks_parquet(num_rows=10000, output_file="ticks.parquet")
