#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
from pathlib import Path

def load_experiment_results():
    """Load timing results from all experiments"""
    results_dir = Path("phase2-spark-analytics/experiments/results")
    results = {}
    
    for result_file in results_dir.glob("*_results.json"):
        with open(result_file, 'r') as f:
            exp_name = result_file.stem.replace('_results', '')
            results[exp_name] = json.load(f)
    
    return results

def generate_cdf_plot(results):
    """Generate CDF plot for experiment results"""
    plt.figure(figsize=(12, 8))
    
    for exp_name, data in results.items():
        times = data['execution_times']
        sorted_times = np.sort(times)
        cdf = np.arange(1, len(sorted_times) + 1) / len(sorted_times)
        
        plt.plot(sorted_times, cdf, label=f'{exp_name}', linewidth=2)
    
    plt.xlabel('Execution Time (seconds)', fontsize=12)
    plt.ylabel('Cumulative Probability', fontsize=12)
    plt.title('Spark MapReduce Execution Time CDF\n(Different M/R Configurations)', fontsize=14)
    plt.legend(fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save plot
    plots_dir = Path("phase2-spark-analytics/experiments/plots")
    plots_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(plots_dir / 'cdf_plot.png', dpi=300, bbox_inches='tight')
    plt.savefig(plots_dir / 'cdf_plot.pdf', bbox_inches='tight')
    plt.show()

def generate_performance_summary(results):
    """Generate performance summary table and plots"""
    summary_data = []
    
    for exp_name, data in results.items():
        times = data['execution_times']
        summary = {
            'Experiment': exp_name,
            'Mean (s)': np.mean(times),
            'Std Dev (s)': np.std(times),
            'P90 (s)': np.percentile(times, 90),
            'P95 (s)': np.percentile(times, 95),
            'P99 (s)': np.percentile(times, 99),
            'Min (s)': np.min(times),
            'Max (s)': np.max(times)
        }
        summary_data.append(summary)
    
    df = pd.DataFrame(summary_data)
    print("Performance Summary:")
    print(df.to_string(index=False))
    
    # Save to CSV
    results_dir = Path("phase2-spark-analytics/experiments/results")
    df.to_csv(results_dir / 'performance_summary.csv', index=False)
    
    return df

if __name__ == "__main__":
    results = load_experiment_results()
    summary_df = generate_performance_summary(results)
    generate_cdf_plot(results)