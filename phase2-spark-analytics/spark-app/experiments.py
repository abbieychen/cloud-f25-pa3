#!/usr/bin/env python3
import time
import json
import logging
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class ExperimentRunner:
    def __init__(self, spark, load_analyzer, work_analyzer):
        self.spark = spark
        self.load_analyzer = load_analyzer
        self.work_analyzer = work_analyzer
        self.results_dir = Path("/opt/spark/experiments/results")
        self.plots_dir = Path("/opt/spark/experiments/plots")
        
        # Create directories
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.plots_dir.mkdir(parents=True, exist_ok=True)
    
    def run_single_experiment(self, config: Dict[str, Any], iteration: int) -> Dict[str, Any]:
        """Run a single experiment iteration"""
        logger.info(f"Running experiment {config['name']} - Iteration {iteration}")
        
        M = config['M']  # Number of partitions for mapping
        R = config['R']  # Number of partitions for reduction
        
        # Set Spark configuration
        self.spark.conf.set("spark.sql.shuffle.partitions", R)
        
        start_time = time.time()
        
        try:
            # Run both analyses
            load_result = self.load_analyzer.compute_average_load(num_partitions=M)
            work_result = self.work_analyzer.compute_total_work(num_partitions=M)
            
            # Force execution by collecting results
            load_count = load_result.count()
            work_count = work_result.count()
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            logger.info(f"Experiment {config['name']} - Iteration {iteration} completed in {execution_time:.2f}s")
            logger.info(f"  Load analysis: {load_count} plugs, Work analysis: {work_count} houses")
            
            return {
                'experiment': config['name'],
                'iteration': iteration,
                'execution_time': execution_time,
                'load_count': load_count,
                'work_count': work_count,
                'M': M,
                'R': R,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Experiment {config['name']} - Iteration {iteration} failed: {e}")
            return {
                'experiment': config['name'],
                'iteration': iteration,
                'execution_time': None,
                'error': str(e),
                'success': False
            }
    
    def run_experiments(self, configs: List[Dict[str, Any]], iterations: int = 10) -> Dict[str, Any]:
        """Run all experiments with multiple iterations"""
        all_results = {}
        
        for config in configs:
            experiment_name = config['name']
            logger.info(f"Starting experiment series: {experiment_name}")
            
            execution_times = []
            detailed_results = []
            
            for i in range(iterations):
                result = self.run_single_experiment(config, i + 1)
                detailed_results.append(result)
                
                if result['success']:
                    execution_times.append(result['execution_time'])
                
                # Small delay between iterations
                time.sleep(2)
            
            # Store results for this experiment
            all_results[experiment_name] = {
                'config': config,
                'execution_times': execution_times,
                'detailed_results': detailed_results,
                'summary': self._compute_summary(execution_times)
            }
            
            logger.info(f"Completed experiment series: {experiment_name}")
            logger.info(f"Summary: {all_results[experiment_name]['summary']}")
        
        return all_results
    
    def _compute_summary(self, execution_times: List[float]) -> Dict[str, float]:
        """Compute statistical summary of execution times"""
        if not execution_times:
            return {}
        
        times_array = np.array(execution_times)
        
        return {
            'mean': float(np.mean(times_array)),
            'std_dev': float(np.std(times_array)),
            'min': float(np.min(times_array)),
            'max': float(np.max(times_array)),
            'p90': float(np.percentile(times_array, 90)),
            'p95': float(np.percentile(times_array, 95)),
            'p99': float(np.percentile(times_array, 99))
        }
    
    def save_results(self, results: Dict[str, Any]):
        """Save experiment results to JSON files"""
        for exp_name, exp_data in results.items():
            output_file = self.results_dir / f"{exp_name}_results.json"
            
            with open(output_file, 'w') as f:
                json.dump(exp_data, f, indent=2)
            
            logger.info(f"Saved results for {exp_name} to {output_file}")
        
        # Save combined results
        combined_file = self.results_dir / "all_experiments_results.json"
        with open(combined_file, 'w') as f:
            json.dump(results, f, indent=2)
    
    def generate_plots(self, results: Dict[str, Any]):
        """Generate CDF and performance plots"""
        self._generate_cdf_plot(results)
        self._generate_performance_comparison(results)
        self._generate_summary_table(results)
    
    def _generate_cdf_plot(self, results: Dict[str, Any]):
        """Generate CDF plot for execution times"""
        plt.figure(figsize=(12, 8))
        
        for exp_name, exp_data in results.items():
            times = exp_data['execution_times']
            if times:
                sorted_times = np.sort(times)
                cdf = np.arange(1, len(sorted_times) + 1) / len(sorted_times)
                
                plt.plot(sorted_times, cdf, label=exp_name, linewidth=2, marker='o', markersize=4)
        
        plt.xlabel('Execution Time (seconds)', fontsize=12)
        plt.ylabel('Cumulative Probability', fontsize=12)
        plt.title('Spark MapReduce Execution Time CDF\n(Different M/R Configurations)', fontsize=14)
        plt.legend(fontsize=10)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save plot
        plot_file = self.plots_dir / 'cdf_plot.png'
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Generated CDF plot: {plot_file}")
    
    def _generate_performance_comparison(self, results: Dict[str, Any]):
        """Generate performance comparison box plot"""
        plt.figure(figsize=(12, 8))
        
        data_to_plot = []
        labels = []
        
        for exp_name, exp_data in results.items():
            times = exp_data['execution_times']
            if times:
                data_to_plot.append(times)
                labels.append(exp_name)
        
        plt.boxplot(data_to_plot, labels=labels)
        plt.xlabel('Experiment Configuration', fontsize=12)
        plt.ylabel('Execution Time (seconds)', fontsize=12)
        plt.title('Spark MapReduce Performance Comparison', fontsize=14)
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save plot
        plot_file = self.plots_dir / 'performance_comparison.png'
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Generated performance comparison plot: {plot_file}")
    
    def _generate_summary_table(self, results: Dict[str, Any]):
        """Generate summary table as a plot"""
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.axis('tight')
        ax.axis('off')
        
        table_data = [['Experiment', 'Mean (s)', 'Std Dev (s)', 'P90 (s)', 'P95 (s)', 'P99 (s)', 'Min (s)', 'Max (s)']]
        
        for exp_name, exp_data in results.items():
            summary = exp_data['summary']
            if summary:
                row = [
                    exp_name,
                    f"{summary['mean']:.2f}",
                    f"{summary['std_dev']:.2f}",
                    f"{summary['p90']:.2f}",
                    f"{summary['p95']:.2f}",
                    f"{summary['p99']:.2f}",
                    f"{summary['min']:.2f}",
                    f"{summary['max']:.2f}"
                ]
                table_data.append(row)
        
        table = ax.table(cellText=table_data, loc='center', cellLoc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.5)
        
        plt.title('Spark MapReduce Performance Summary', fontsize=14, pad=20)
        
        # Save table
        table_file = self.plots_dir / 'performance_summary.png'
        plt.savefig(table_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Generated performance summary table: {table_file}")