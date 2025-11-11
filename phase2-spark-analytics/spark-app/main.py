#!/usr/bin/env python3
import time
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="EnergyDataAnalytics"):
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def compute_average_load_per_plug(spark, num_partitions):
    """Compute average load for each unique plug"""
    logger.info("Computing average load per plug...")
    
    # Sample data generation for testing
    data = []
    for i in range(1000):
        record = (
            i,  # id
            1609459200 + i,  # timestamp
            float(100 + (i % 50) * 10),  # value
            i % 2,  # property (0=work, 1=load)
            (i % 5) + 1,  # plug_id
            (i % 3) + 1,  # household_id
            (i % 2) + 1   # house_id
        )
        data.append(record)
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("value", FloatType(), True),
        StructField("property", IntegerType(), True),
        StructField("plug_id", IntegerType(), True),
        StructField("household_id", IntegerType(), True),
        StructField("house_id", IntegerType(), True)
    ])
    
    # Create DataFrame from sample data
    df = spark.createDataFrame(data, schema)
    
    # Filter for load measurements (property=1) and compute average
    result = df.filter(F.col("property") == 1) \
        .repartition(num_partitions) \
        .groupBy("house_id", "household_id", "plug_id") \
        .agg(F.avg("value").alias("average_load")) \
        .orderBy("house_id", "household_id", "plug_id")
    
    logger.info(f"Computed average load for {result.count()} unique plugs")
    return result

def compute_total_work_per_house(spark, num_partitions):
    """Compute total work for each house"""
    logger.info("Computing total work per house...")
    
    # Sample data generation for testing
    data = []
    work_accumulated = 0.0
    for i in range(1000):
        if i % 2 == 0:  # Work measurements (property=0)
            work_accumulated += float(0.1 + (i % 10) * 0.05)
            record = (
                i,  # id
                1609459200 + i,  # timestamp
                work_accumulated,  # value (cumulative)
                0,  # property (0=work)
                (i % 5) + 1,  # plug_id
                (i % 3) + 1,  # household_id
                (i % 2) + 1   # house_id
            )
            data.append(record)
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("value", FloatType(), True),
        StructField("property", IntegerType(), True),
        StructField("plug_id", IntegerType(), True),
        StructField("household_id", IntegerType(), True),
        StructField("house_id", IntegerType(), True)
    ])
    
    # Create DataFrame from sample data
    df = spark.createDataFrame(data, schema)
    
    # Get latest work measurement per plug and sum per house
    latest_work = df.filter(F.col("property") == 0) \
        .groupBy("house_id", "household_id", "plug_id") \
        .agg(F.max("value").alias("latest_work"))
    
    total_work = latest_work.groupBy("house_id") \
        .agg(F.sum("latest_work").alias("total_work")) \
        .orderBy("house_id")
    
    logger.info(f"Computed total work for {total_work.count()} houses")
    return total_work

def run_experiments(spark, experiment_configs, iterations=10):
    """Run experiments with different configurations"""
    results = {}
    
    for config in experiment_configs:
        experiment_name = config['name']
        M = config['M']
        R = config['R']
        
        logger.info(f"Starting experiment: {experiment_name} (M={M}, R={R})")
        
        execution_times = []
        
        for i in range(iterations):
            logger.info(f"  Iteration {i+1}/{iterations}")
            
            # Set Spark configuration
            spark.conf.set("spark.sql.shuffle.partitions", R)
            
            start_time = time.time()
            
            # Run both analyses
            load_result = compute_average_load_per_plug(spark, M)
            work_result = compute_total_work_per_house(spark, M)
            
            # Force execution by collecting results
            load_count = load_result.count()
            work_count = work_result.count()
            
            end_time = time.time()
            execution_time = end_time - start_time
            execution_times.append(execution_time)
            
            logger.info(f"    Completed in {execution_time:.2f}s (loads: {load_count}, works: {work_count})")
            
            # Small delay between iterations
            time.sleep(2)
        
        # Store results
        times_array = np.array(execution_times)
        results[experiment_name] = {
            'config': config,
            'execution_times': execution_times,
            'summary': {
                'mean': float(np.mean(times_array)),
                'std_dev': float(np.std(times_array)),
                'min': float(np.min(times_array)),
                'max': float(np.max(times_array)),
                'p90': float(np.percentile(times_array, 90)),
                'p95': float(np.percentile(times_array, 95)),
                'p99': float(np.percentile(times_array, 99))
            }
        }
        
        logger.info(f"Completed experiment {experiment_name}")
        logger.info(f"  Mean time: {results[experiment_name]['summary']['mean']:.2f}s")
    
    return results

def generate_plots(results):
    """Generate CDF plots for experiment results"""
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
    plots_dir = Path("/opt/spark/experiments/plots")
    plots_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(plots_dir / 'cdf_plot.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info("Generated CDF plot")

def main():
    logger.info("Starting Energy Data Analytics Application")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Experiment configurations
        experiment_configs = [
            {'M': 10, 'R': 2, 'name': 'M10_R2'},
            {'M': 50, 'R': 5, 'name': 'M50_R5'},
            {'M': 100, 'R': 10, 'name': 'M100_R10'}
        ]
        
        # Run experiments
        results = run_experiments(spark, experiment_configs, iterations=10)
        
        # Save results
        results_dir = Path("/opt/spark/experiments/results")
        results_dir.mkdir(parents=True, exist_ok=True)
        
        for exp_name, data in results.items():
            with open(results_dir / f"{exp_name}_results.json", 'w') as f:
                json.dump(data, f, indent=2)
        
        # Generate plots
        generate_plots(results)
        
        logger.info("Energy Data Analytics completed successfully")
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
