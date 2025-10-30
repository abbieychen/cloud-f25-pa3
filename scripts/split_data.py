import pandas as pd
import numpy as np

def split_large_dataset(input_file, num_parts=5):
    """Split 100M record dataset into equal parts for publishers"""
    # Calculate rows per part
    total_rows = 100000000
    rows_per_part = total_rows // num_parts
    
    for i in range(num_parts):
        start_row = i * rows_per_part
        end_row = (i + 1) * rows_per_part
        
        # Read chunk and save
        chunk = pd.read_csv(input_file, skiprows=range(1, start_row), nrows=rows_per_part)
        chunk.to_csv(f'data/split/part_{i+1}.csv', index=False)
        print(f"Created part_{i+1}.csv with {len(chunk)} rows")