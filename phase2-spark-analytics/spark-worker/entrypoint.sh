#!/bin/bash

# Set Spark configuration
export SPARK_MASTER_URL="spark://spark-master:7077"
export SPARK_WORKER_MEMORY="2g"
export SPARK_WORKER_CORES="2"
export SPARK_WORKER_WEBUI_PORT="8081"

# Wait for master to be ready
echo "Waiting for Spark Master to be ready..."
sleep 30

# Start Spark Worker
echo "Starting Spark Worker connecting to $SPARK_MASTER_URL..."
/opt/spark/sbin/start-worker.sh $SPARK_MASTER_URL --webui-port 8081

# Keep the container running
tail -f /opt/spark/logs/spark--org.apache.spark.deploy.worker.Worker-*.out
