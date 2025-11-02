#!/usr/bin/env python3
import yaml
import json
import time
from pathlib import Path

def load_experiment_configs():
    """Load all experiment configurations"""
    config_dir = Path("config")
    configs = []
    
    for config_file in config_dir.glob("*.yaml"):
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            configs.append(config)
    
    return configs

def generate_sample_results():
    """Generate sample results for testing"""
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)
    
    experiment_configs = [
        {"name": "M10_R2", "M": 10, "R": 2},
        {"name": "M50_R5", "M": 50, "R": 5},
        {"name": "M100_R10", "M": 100, "R": 10}
    ]
    
    for config in experiment_configs:
        # Generate sample execution times
        import numpy as np
        np.random.seed(42)
        
        # Different base times for different configurations
        if config["M"] == 10:
            base_time = 120
            variation = 20
        elif config["M"] == 50:
            base_time = 90
            variation = 15
        else:  # M=100
            base_time = 75
            variation = 10
        
        execution_times = np.random.normal(base_time, variation, 10)
        execution_times = [max(60, t) for t in execution_times]  # Ensure minimum time
        
        results = {
            'experiment_name': config['name'],
            'parameters': {
                'M': config['M'],
                'R': config['R']
            },
            'execution_times': execution_times.tolist(),
            'summary': {
                'mean': float(np.mean(execution_times)),
                'std_dev': float(np.std(execution_times)),
                'min': float(np.min(execution_times)),
                'max': float(np.max(execution_times)),
                'p90': float(np.percentile(execution_times, 90)),
                'p95': float(np.percentile(execution_times, 95)),
                'p99': float(np.percentile(execution_times, 99))
            },
            'timestamp': time.time()
        }
        
        # Save results
        output_file = results_dir / f"{config['name']}_results.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"Generated sample results for {config['name']}")

if __name__ == "__main__":
    generate_sample_results()