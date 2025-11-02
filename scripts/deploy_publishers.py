#!/usr/bin/env python3
import subprocess
import time
import os
from pathlib import Path

def deploy_publishers():
    """Deploy 5 publisher containers across different hosts"""
    print("Deploying publishers...")
    
    publishers_config = [
        {"name": "publisher1", "port": 8001, "data_file": "data/split/part_1.csv"},
        {"name": "publisher2", "port": 8002, "data_file": "data/split/part_2.csv"},
        {"name": "publisher3", "port": 8003, "data_file": "data/split/part_3.csv"},
        {"name": "publisher4", "port": 8004, "data_file": "data/split/part_4.csv"},
        {"name": "publisher5", "port": 8005, "data_file": "data/split/part_5.csv"},
    ]
    
    for config in publishers_config:
        print(f"Starting {config['name']}...")
        
        # Build and run publisher container
        cmd = [
            "docker", "run", "-d",
            "--name", config["name"],
            "-p", f"{config['port']}:5000",
            "-v", f"{os.path.abspath(config['data_file'])}:/app/data.csv",
            "-e", "KAFKA_BROKERS=localhost:9092,localhost:9093",
            "energy-publisher:latest"
        ]
        
        try:
            subprocess.run(cmd, check=True)
            print(f"✓ {config['name']} deployed successfully")
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to deploy {config['name']}: {e}")
        
        time.sleep(2)
    
    print("All publishers deployed!")

if __name__ == "__main__":
    deploy_publishers()