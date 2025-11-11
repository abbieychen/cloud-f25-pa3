#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np
import json
import seaborn as sns
from pathlib import Path

def load_results():
    """Load experiment results"""
    results_dir = Path("../results")
    results = {}
    
    for result_file in results_dir.glob("*_results.json"):
        with open(result_file, 'r') as f:
            exp_name = result_file.stem.replace('_results', '')
            results[exp_name] = json.load(f)
    
    return results

def create_comprehensive_plots(results):
    """Create comprehensive visualization of results"""
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (15, 10)
    
    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. CDF Plot
    for exp_name, data in results.items():
        times = data['execution_times']
        sorted_times = np.sort(times)
        cdf = np.arange(1, len(sorted_times) + 1) / len(sorted_times)
        ax1.plot(sorted_times, cdf, label=exp_name, linewidth=2, marker='o', markersize=4)
    
    ax1.set_xlabel('Execution Time (seconds)')
    ax1.set_ylabel('Cumulative Probability')
    ax1.set_title('Execution Time CDF\n(Different M/R Configurations)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Box Plot
    data_to_plot = []
    labels = []
    for exp_name, data in results.items():
        data_to_plot.append(data['execution_times'])
        labels.append(exp_name)
    
    ax2.boxplot(data_to_plot, labels=labels)
    ax2.set_xlabel('Experiment Configuration')
    ax2.set_ylabel('Execution Time (seconds)')
    ax2.set_title('Performance Comparison')
    ax2.grid(True, alpha=0.3)
    
    # 3. Mean Performance Bar Chart
    means = [data['summary']['mean'] for data in results.values()]
    configs = list(results.keys())
    
    bars = ax3.bar(configs, means, color=['skyblue', 'lightgreen', 'lightcoral'])
    ax3.set_xlabel('Configuration')
    ax3.set_ylabel('Mean Execution Time (seconds)')
    ax3.set_title('Average Performance by Configuration')
    
    # Add value labels on bars
    for bar, mean in zip(bars, means):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{mean:.1f}s', ha='center', va='bottom')
    
    # 4. Performance Metrics Comparison
    metrics = ['mean', 'p90', 'p95', 'p99']
    x_pos = np.arange(len(metrics))
    width = 0.25
    
    for i, (exp_name, data) in enumerate(results.items()):
        values = [data['summary'][metric] for metric in metrics]
        ax4.bar(x_pos + i*width, values, width, label=exp_name)
    
    ax4.set_xlabel('Performance Metric')
    ax4.set_ylabel('Time (seconds)')
    ax4.set_title('Performance Metrics Comparison')
    ax4.set_xticks(x_pos + width)
    ax4.set_xticklabels(metrics)
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('comprehensive_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def create_performance_table(results):
    """Create a performance summary table"""
    import pandas as pd
    
    summary_data = []
    for exp_name, data in results.items():
        summary = data['summary']
        summary_data.append({
            'Configuration': exp_name,
            'M': data['parameters']['M'],
            'R': data['parameters']['R'],
            'Mean (s)': f"{summary['mean']:.2f}",
            'Std Dev (s)': f"{summary['std_dev']:.2f}",
            'P90 (s)': f"{summary['p90']:.2f}",
            'P95 (s)': f"{summary['p95']:.2f}",
            'P99 (s)': f"{summary['p99']:.2f}",
            'Min (s)': f"{summary['min']:.2f}",
            'Max (s)': f"{summary['max']:.2f}"
        })
    
    df = pd.DataFrame(summary_data)
    print("Performance Summary Table:")
    print(df.to_string(index=False))
    
    # Save as CSV
    df.to_csv('performance_summary.csv', index=False)
    
    return df

if __name__ == "__main__":
    results = load_results()
    create_comprehensive_plots(results)
    create_performance_table(results)