#!/usr/bin/env python3
import pandas as pd
import os
from pathlib import Path

def split_large_dataset():
    print("starting")
    """Split the energy-sorted1M.csv dataset into 5 parts for publishers"""
    
    # Define the exact path to your dataset
    input_file = Path('/Users/abbiechen/Desktop/cloud2025/cloud-f25-pa3/data/raw/energy-sorted1M.csv')
    output_dir = Path('/Users/abbiechen/Desktop/cloud2025/cloud-f25-pa3/data/split')
    num_parts = 5
    
    print(f"Splitting {input_file} into {num_parts} parts...")
    
    # Check if input file exists
    if not input_file.exists():
        print(f"ERROR: Input file {input_file} does not exist!")
        return
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read the entire dataset
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file)
    total_rows = len(df)
    rows_per_part = total_rows // num_parts
    
    print(f"Total records: {total_rows:,}")
    print(f"Records per part: {rows_per_part:,}")
    
    for i in range(num_parts):
        start_idx = i * rows_per_part
        end_idx = (i + 1) * rows_per_part if i < num_parts - 1 else total_rows
        
        part_df = df.iloc[start_idx:end_idx]
        output_file = output_dir / f'part_{i+1}.csv'
        part_df.to_csv(output_file, index=False)
        print(f"Created {output_file} with {len(part_df):,} records")
    
    print("Data splitting completed!")
    print(f"Split files created in: {output_dir}")

if __name__ == "__main__":
    split_large_dataset()