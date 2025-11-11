#!/bin/bash

# Wait for Spark Master to be ready
echo "Waiting for Spark Master to be ready..."
sleep 60

# Run the Spark application
echo "Starting Spark Application..."
spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 \
    --conf spark.dynamicAllocation.maxExecutors=18 \
    --conf spark.dynamicAllocation.initialExecutors=2 \
    --py-files /opt/spark/apps/analytics.py,/opt/spark/apps/database_connector.py \
    /opt/spark/apps/main.py