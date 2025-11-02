#!/bin/bash

# Set Spark configuration
export SPARK_MASTER_HOST="spark-master"
export SPARK_MASTER_PORT="7077"
export SPARK_MASTER_WEBUI_PORT="8080"

# Create spark configuration directory if it doesn't exist
mkdir -p /opt/spark/conf

# Generate spark-defaults.conf if it doesn't exist
if [ ! -f /opt/spark/conf/spark-defaults.conf ]; then
    cat > /opt/spark/conf/spark-defaults.conf << 'EOL'
spark.master                            spark://spark-master:7077
spark.driver.memory                     2g
spark.executor.memory                   2g
spark.executor.cores                    2
spark.sql.adaptive.enabled              true
spark.sql.adaptive.coalescePartitions.enabled true
spark.serializer                        org.apache.spark.serializer.KryoSerializer
spark.sql.catalogImplementation         hive
EOL
fi

# Start Spark Master
echo "Starting Spark Master..."
/opt/spark/sbin/start-master.sh -h spark-master -p 7077 --webui-port 8080

# Keep the container running
tail -f /opt/spark/logs/spark--org.apache.spark.deploy.master.Master-*.out
