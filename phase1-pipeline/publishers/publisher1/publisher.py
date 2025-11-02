#!/usr/bin/env python3
import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchPublisher:
    def __init__(self, broker_list, batch_size=10, topic='sensor-data'):
        self.batch_size = batch_size
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker_list,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            request_timeout_ms=30000
        )
        logger.info(f"Publisher 1 initialized with {len(broker_list)} brokers")

    def publish_data(self, data_file):
        """Publish data from CSV file"""
        try:
            # Read CSV file
            df = pd.read_csv(data_file)
            total_records = len(df)
            logger.info(f"Publisher 1: Loaded {total_records} records from {data_file}")

            success_count = 0
            for _, row in df.iterrows():
                record = {
                    'id': int(row['id']),
                    'timestamp': int(row['timestamp']),
                    'value': float(row['value']),
                    'property': int(row['property']),
                    'plug_id': int(row['plug_id']),
                    'household_id': int(row['household_id']),
                    'house_id': int(row['house_id'])
                }
                
                # Send each record individually with confirmation
                future = self.producer.send(self.topic, value=record)
                future.get(timeout=10)  # Wait for confirmation
                success_count += 1
                logger.info(f"Publisher 1: Successfully sent record {record['id']} ({success_count}/{total_records})")
                time.sleep(0.2)  # Slow down to see progress

            # Wait for all messages to be delivered
            self.producer.flush()
            logger.info(f"Publisher 1: Completed publishing {success_count} records")

        except Exception as e:
            logger.error(f"Publisher 1: Error publishing data: {e}")
            raise

def wait_for_kafka(broker_list, max_retries=30, retry_interval=5):
    """Wait for Kafka brokers to be available"""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker_list,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            # Test connection
            future = producer.send('test-connection', b'test')
            future.get(timeout=10)
            producer.close()
            logger.info(f"Kafka brokers are available after {attempt * retry_interval} seconds")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Kafka not ready, waiting {retry_interval} seconds...")
            time.sleep(retry_interval)
    return False

def main():
    # Configuration - using 2 Kafka brokers in Docker network
    KAFKA_BROKERS = ['kafka1:9092', 'kafka2:9093']
    DATA_FILE = os.getenv('DATA_FILE', '/app/data.csv')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))

    logger.info(f"Publisher 1: Starting with brokers: {KAFKA_BROKERS}")
    logger.info(f"Publisher 1: Data file: {DATA_FILE}, Batch size: {BATCH_SIZE}")

    # Wait for Kafka to be ready
    logger.info("Publisher 1: Waiting for Kafka brokers to be ready...")
    if not wait_for_kafka(KAFKA_BROKERS, max_retries=40, retry_interval=5):
        logger.error("Publisher 1: Failed to connect to Kafka brokers after multiple attempts")
        return

    # Create and run publisher
    publisher = BatchPublisher(KAFKA_BROKERS, batch_size=BATCH_SIZE)
    publisher.publish_data(DATA_FILE)

if __name__ == "__main__":
    main()