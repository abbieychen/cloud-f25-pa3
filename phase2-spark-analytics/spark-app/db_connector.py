#!/usr/bin/env python3
import mysql.connector
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
    
    def get_connection(self):
        """Create database connection"""
        return mysql.connector.connect(**self.db_config)
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a specific table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get data distribution
            cursor.execute(f"""
                SELECT house_id, COUNT(*) as count 
                FROM {table_name} 
                GROUP BY house_id 
                ORDER BY house_id
            """)
            house_distribution = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'house_distribution': dict(house_distribution)
            }
            
        except Exception as e:
            logger.error(f"Error getting stats for {table_name}: {e}")
            return {}
    
    def get_all_shard_stats(self) -> List[Dict[str, Any]]:
        """Get statistics for all shard tables"""
        stats = []
        for i in range(5):
            table_name = f"sensor_data_shard_{i}"
            table_stats = self.get_table_stats(table_name)
            stats.append(table_stats)
        
        return stats
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            conn = self.get_connection()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False