#!/usr/bin/env python3
import json
import requests
from kafka import KafkaConsumer
import logging
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_kafka(broker_list, max_retries=30, retry_interval=5):
    """Wait for Kafka brokers to be available"""
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=broker_list,
                group_id='test-group'
            )
            consumer.close()
            logger.info(f"Kafka brokers are available after {attempt * retry_interval} seconds")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Kafka not ready, waiting {retry_interval} seconds...")
            time.sleep(retry_interval)
    return False

def main():
    KAFKA_BROKERS = ['kafka1:9092', 'kafka2:9093']
    WEB_SERVER_URL = 'http://webserver:5000'
    
    logger.info(f"Subscriber starting with brokers: {KAFKA_BROKERS}")
    logger.info(f"Web server URL: {WEB_SERVER_URL}")

    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka brokers to be ready...")
    if not wait_for_kafka(KAFKA_BROKERS):
        logger.error("Failed to connect to Kafka brokers after multiple attempts")
        return

    # Wait for web server to be ready
    logger.info("Waiting for web server to be ready...")
    for attempt in range(30):
        try:
            response = requests.get(f"{WEB_SERVER_URL}/health", timeout=5)
            if response.status_code == 200:
                logger.info("Web server is ready")
                break
        except:
            logger.warning(f"Web server not ready yet, attempt {attempt + 1}/30")
            time.sleep(5)
    else:
        logger.error("Web server never became ready")
        return

    try:
        consumer = KafkaConsumer(
            'sensor-data',
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='energy-data-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info("Subscriber started successfully, waiting for messages...")
        
        for message in consumer:
            try:
                record = message.value
                response = requests.post(
                    f"{WEB_SERVER_URL}/sensor-data",
                    json=record,
                    timeout=5
                )
                
                if response.status_code == 200:
                    logger.info(f"Successfully processed record {record['id']}")
                else:
                    logger.warning(f"Failed to process record {record['id']}: {response.status_code}")
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Error sending data to web server: {e}")
            except Exception as e:
                logger.error(f"Unexpected error processing message: {e}")
    
    except KeyboardInterrupt:
        logger.info("Shutting down subscriber...")
    except Exception as e:
        logger.error(f"Subscriber error: {e}")

if __name__ == "__main__":
    main()