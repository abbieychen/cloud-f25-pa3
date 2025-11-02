#!/usr/bin/env python3
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
import logging

logger = logging.getLogger(__name__)

class AverageLoadAnalyzer:
    def __init__(self, spark, db_config):
        self.spark = spark
        self.db_config = db_config
        self.schema = self._create_schema()
    
    def _create_schema(self):
        """Define the schema for energy data"""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("timestamp", LongType(), True),
            StructField("value", FloatType(), True),
            StructField("property", IntegerType(), True),
            StructField("plug_id", IntegerType(), True),
            StructField("household_id", IntegerType(), True),
            StructField("house_id", IntegerType(), True)
        ])
    
    def read_sharded_data(self, num_partitions=None):
        """Read data from all sharded tables"""
        logger.info("Reading data from sharded tables...")
        
        dfs = []
        for i in range(5):  # 5 shards
            table_name = f"sensor_data_shard_{i}"
            logger.info(f"Reading from {table_name}")
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}") \
                .option("dbtable", table_name) \
                .option("user", self.db_config["user"]) \
                .option("password", self.db_config["password"]) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .schema(self.schema) \
                .load()
            
            if num_partitions:
                df = df.repartition(num_partitions)
            
            dfs.append(df)
        
        # Combine all shards
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)
        
        logger.info(f"Combined data count: {combined_df.count()}")
        return combined_df
    
    def compute_average_load(self, num_partitions=None):
        """Compute average load for each unique plug"""
        logger.info("Computing average load per plug...")
        
        # Read data
        df = self.read_sharded_data(num_partitions)
        
        # Filter for load measurements (property=1) and compute average
        result = df.filter(F.col("property") == 1) \
            .groupBy("house_id", "household_id", "plug_id") \
            .agg(F.avg("value").alias("average_load")) \
            .orderBy("house_id", "household_id", "plug_id")
        
        logger.info(f"Computed average load for {result.count()} unique plugs")
        return result
    
    def save_results(self, result_df, output_path="/opt/spark/results/average_load"):
        """Save results to output path"""
        result_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        logger.info(f"Saved average load results to {output_path}")

class TotalWorkAnalyzer:
    def __init__(self, spark, db_config):
        self.spark = spark
        self.db_config = db_config
        self.schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("timestamp", LongType(), True),
            StructField("value", FloatType(), True),
            StructField("property", IntegerType(), True),
            StructField("plug_id", IntegerType(), True),
            StructField("household_id", IntegerType(), True),
            StructField("house_id", IntegerType(), True)
        ])
    
    def read_sharded_data(self, num_partitions=None):
        """Read data from all sharded tables"""
        dfs = []
        for i in range(5):  # 5 shards
            table_name = f"sensor_data_shard_{i}"
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}") \
                .option("dbtable", table_name) \
                .option("user", self.db_config["user"]) \
                .option("password", self.db_config["password"]) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .schema(self.schema) \
                .load()
            
            if num_partitions:
                df = df.repartition(num_partitions)
            
            dfs.append(df)
        
        # Combine all shards
        combined_df = dfs[0]
        for df in dfs[1:]:
            combined_df = combined_df.union(df)
        
        return combined_df
    
    def compute_total_work(self, num_partitions=None):
        """Compute total work for each house"""
        logger.info("Computing total work per house...")
        
        # Read data
        df = self.read_sharded_data(num_partitions)
        
        # For work measurements (property=0), get the latest value per plug
        latest_work = df.filter(F.col("property") == 0) \
            .groupBy("house_id", "household_id", "plug_id") \
            .agg(
                F.max("value").alias("latest_work"),
                F.max("timestamp").alias("latest_timestamp")
            )
        
        # Sum the latest work values per house
        total_work = latest_work.groupBy("house_id") \
            .agg(F.sum("latest_work").alias("total_work")) \
            .orderBy("house_id")
        
        logger.info(f"Computed total work for {total_work.count()} houses")
        return total_work
    
    def save_results(self, result_df, output_path="/opt/spark/results/total_work"):
        """Save results to output path"""
        result_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        logger.info(f"Saved total work results to {output_path}")