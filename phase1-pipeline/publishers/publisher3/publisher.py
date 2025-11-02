#!/usr/bin/env python3
import json
import time
import pandas as pd
from kafka import KafkaProducer
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchPublisher:
    def __init__(self, broker_list, batch_size=100, topic='sensor-data'):
        self.batch_size = batch_size
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker_list,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            batch_size=16384,
            linger_ms=10
        )
        logger.info(f"Initialized publisher with batch size {batch_size}")

    def publish_data(self, data_file):
        """Publish data from CSV file in batches"""
        try:
            # Read CSV file
            df = pd.read_csv(data_file)
            total_records = len(df)
            logger.info(f"Loaded {total_records} records from {data_file}")

            records_published = 0
            batch_records = []

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
                batch_records.append(record)

                # Publish batch when size reached
                if len(batch_records) >= self.batch_size:
                    self._publish_batch(batch_records)
                    records_published += len(batch_records)
                    batch_records = []
                    logger.info(f"Published {records_published}/{total_records} records")

            # Publish remaining records
            if batch_records:
                self._publish_batch(batch_records)
                records_published += len(batch_records)

            # Wait for all messages to be delivered
            self.producer.flush()
            logger.info(f"Completed publishing {records_published} records")

        except Exception as e:
            logger.error(f"Error publishing data: {e}")
            raise

    def _publish_batch(self, batch):
        """Publish a batch of records"""
        for record in batch:
            self.producer.send(self.topic, value=record)
        
        logger.debug(f"Sent batch of {len(batch)} records")

def main():
    # Configuration
    KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'localhost:9092,localhost:9093').split(',')
    DATA_FILE = os.getenv('DATA_FILE', '/app/data.csv')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))

    logger.info(f"Starting publisher with brokers: {KAFKA_BROKERS}")
    logger.info(f"Data file: {DATA_FILE}, Batch size: {BATCH_SIZE}")

    # Create and run publisher
    publisher = BatchPublisher(KAFKA_BROKERS, batch_size=BATCH_SIZE)
    
    # Wait for Kafka to be ready
    time.sleep(10)
    
    publisher.publish_data(DATA_FILE)

if __name__ == "__main__":
    main()