import mysql.connector
import random
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class ShardedDatabase:
    def __init__(self, db_config: Dict[str, str], num_shards: int = 5):
        self.db_config = db_config
        self.num_shards = num_shards
        self.shard_tables = [f'sensor_data_shard_{i}' for i in range(num_shards)]
        self._initialize_database()

    def _get_connection(self):
        """Create a new database connection"""
        return mysql.connector.connect(**self.db_config)

    def _initialize_database(self):
        """Initialize database and create shard tables"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create shard tables
            for table_name in self.shard_tables:
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT UNSIGNED,
                    timestamp INT UNSIGNED,
                    value FLOAT,
                    property BOOLEAN,
                    plug_id INT UNSIGNED,
                    household_id INT UNSIGNED,
                    house_id INT UNSIGNED,
                    PRIMARY KEY (id),
                    INDEX idx_house (house_id, household_id, plug_id),
                    INDEX idx_timestamp (timestamp),
                    INDEX idx_property (property)
                )
                """
                cursor.execute(create_table_sql)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("Database tables initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def get_random_shard(self) -> str:
        """Select a random shard table"""
        return random.choice(self.shard_tables)

    def store_record(self, record: Dict[str, Any]) -> bool:
        """Store a record in a random shard"""
        try:
            shard_table = self.get_random_shard()
            conn = self._get_connection()
            cursor = conn.cursor()
            
            insert_sql = f"""
            INSERT INTO {shard_table} (id, timestamp, value, property, plug_id, household_id, house_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                record['id'],
                record['timestamp'],
                record['value'],
                record['property'],
                record['plug_id'],
                record['household_id'],
                record['house_id']
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing record {record.get('id')} in {shard_table}: {e}")
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about data distribution across shards"""
        stats = {}
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            total_records = 0
            shard_counts = {}
            
            for table_name in self.shard_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                shard_counts[table_name] = count
                total_records += count
            
            stats['total_records'] = total_records
            stats['shard_distribution'] = shard_counts
            stats['num_shards'] = self.num_shards
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            stats['error'] = str(e)
        
        return stats