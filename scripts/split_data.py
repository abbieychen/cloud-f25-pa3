#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
from pathlib import Path

def generate_sample_data():
    """Generate sample data matching the energy dataset schema"""
    print("Generating sample energy data...")
    
    # Create sample data with 1000 records for testing
    np.random.seed(42)
    n_records = 1000
    
    data = {
        'id': range(1, n_records + 1),
        'timestamp': np.random.randint(1609459200, 1640995200, n_records),  # 2021-2022 timestamps
        'value': np.random.uniform(0, 5000, n_records),
        'property': np.random.choice([0, 1], n_records),  # 0=work, 1=load
        'plug_id': np.random.randint(1, 51, n_records),
        'household_id': np.random.randint(1, 11, n_records),
        'house_id': np.random.randint(1, 6, n_records)
    }
    
    df = pd.DataFrame(data)
    df.to_csv('data/raw/energy_dataset_100M.csv', index=False)
    print(f"Generated sample data with {n_records} records")
    return df

def split_large_dataset(input_file, num_parts=5):
    """Split dataset into equal parts for publishers"""
    print(f"Splitting {input_file} into {num_parts} parts...")
    
    # Create output directory
    output_dir = Path('data/split')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read the entire dataset
    df = pd.read_csv(input_file)
    total_rows = len(df)
    rows_per_part = total_rows // num_parts
    
    print(f"Total records: {total_rows}")
    print(f"Records per part: {rows_per_part}")
    
    for i in range(num_parts):
        start_idx = i * rows_per_part
        end_idx = (i + 1) * rows_per_part if i < num_parts - 1 else total_rows
        
        part_df = df.iloc[start_idx:end_idx]
        output_file = output_dir / f'part_{i+1}.csv'
        part_df.to_csv(output_file, index=False)
        print(f"Created {output_file} with {len(part_df)} records")
    
    print("Data splitting completed!")

if __name__ == "__main__":
    # Generate sample data if doesn't exist
    data_file = 'data/raw/energy_dataset_100M.csv'
    if not os.path.exists(data_file):
        generate_sample_data()
    
    # Split the data
    print("splitting data!")
    split_large_dataset(data_file, num_parts=5)