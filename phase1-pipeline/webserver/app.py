#!/usr/bin/env python3
from flask import Flask, request, jsonify
from models import ShardedDatabase
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize database
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'energy_user'),
    'password': os.getenv('DB_PASSWORD', 'energy_pass'),
    'database': os.getenv('DB_NAME', 'energy_db')
}

db = ShardedDatabase(db_config, num_shards=5)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "energy-webserver"})

@app.route('/sensor-data', methods=['POST'])
def receive_sensor_data():
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['id', 'timestamp', 'value', 'property', 'plug_id', 'household_id', 'house_id']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing field: {field}"}), 400
        
        # Store record in random shard
        success = db.store_record(data)
        
        if success:
            logger.debug(f"Stored record {data['id']} successfully")
            return jsonify({"status": "success", "record_id": data['id']})
        else:
            logger.error(f"Failed to store record {data['id']}")
            return jsonify({"error": "Database storage failed"}), 500
            
    except Exception as e:
        logger.error(f"Error processing sensor data: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get statistics about stored data"""
    try:
        stats = db.get_statistics()
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return jsonify({"error": "Failed to get statistics"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)