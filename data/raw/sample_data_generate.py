#!/usr/bin/env python3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sample_energy_data(num_records=1000000):
    """Generate sample energy data matching the DEBS 2014 schema"""
    np.random.seed(42)
    
    # Base parameters
    num_houses = 5
    num_households_per_house = 3
    num_plugs_per_household = 10
    
    # Generate base entities
    houses = [f"house_{i}" for i in range(1, num_houses + 1)]
    households = [f"household_{i}" for i in range(1, num_houses * num_households_per_house + 1)]
    plugs = [f"plug_{i}" for i in range(1, num_houses * num_households_per_house * num_plugs_per_household + 1)]
    
    data = []
    current_id = 1
    
    # Start timestamp (January 1, 2021)
    start_timestamp = int(datetime(2021, 1, 1).timestamp())
    
    for house_idx in range(num_houses):
        house_id = house_idx + 1
        
        for household_idx in range(num_households_per_house):
            household_id = house_idx * num_households_per_house + household_idx + 1
            
            for plug_idx in range(num_plugs_per_household):
                plug_id = (house_idx * num_households_per_house * num_plugs_per_household + 
                          household_idx * num_plugs_per_household + plug_idx + 1)
                
                # Generate data for this plug over time
                current_time = start_timestamp
                work_accumulated = 0.0
                
                for time_step in range(100):  # 100 time steps per plug
                    # Generate load measurement (property=1)
                    load_value = np.random.uniform(100, 2000)
                    
                    data.append({
                        'id': current_id,
                        'timestamp': current_time,
                        'value': load_value,
                        'property': 1,  # Load measurement
                        'plug_id': plug_id,
                        'household_id': household_id,
                        'house_id': house_id
                    })
                    current_id += 1
                    
                    # Generate work measurement (property=0) - cumulative
                    work_increment = np.random.uniform(0.1, 0.5)
                    work_accumulated += work_increment
                    
                    data.append({
                        'id': current_id,
                        'timestamp': current_time + 1,  # Slightly different timestamp
                        'value': work_accumulated,
                        'property': 0,  # Work measurement
                        'plug_id': plug_id,
                        'household_id': household_id,
                        'house_id': house_id
                    })
                    current_id += 1
                    
                    # Move to next time step (1 hour later)
                    current_time += 3600
    
    df = pd.DataFrame(data)
    return df

if __name__ == "__main__":
    print("Generating sample energy data...")
    df = generate_sample_energy_data(1000000)
    
    # Save to CSV
    output_file = "energy_dataset_100M.csv"
    df.to_csv(output_file, index=False)
    print(f"Generated {len(df)} records and saved to {output_file}")
    
    # Print some statistics
    print(f"\nDataset Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Unique houses: {df['house_id'].nunique()}")
    print(f"Unique households: {df['household_id'].nunique()}")
    print(f"Unique plugs: {df['plug_id'].nunique()}")
    print(f"Load measurements: {len(df[df['property'] == 1])}")
    print(f"Work measurements: {len(df[df['property'] == 0])}")