#!/bin/bash
echo "=== PHASE 1 FINAL VERIFICATION ==="

echo "1. Container Status:"
docker ps --format "table {{.Names}}\t{{.Status}}"

echo -e "\n2. Database Records:"
docker exec phase1-pipeline-mysql-1 mysql -u energy_user -penergy_pass energy_db -e "
SELECT 'shard_0' as shard, COUNT(*) as count FROM sensor_data_shard_0 
UNION SELECT 'shard_1', COUNT(*) FROM sensor_data_shard_1 
UNION SELECT 'shard_2', COUNT(*) FROM sensor_data_shard_2 
UNION SELECT 'shard_3', COUNT(*) FROM sensor_data_shard_3 
UNION SELECT 'shard_4', COUNT(*) FROM sensor_data_shard_4;" 2>/dev/null

echo -e "\n3. Service Health:"
curl -s http://localhost:5001/health | python3 -m json.tool

echo -e "\n4. Recent Activity:"
echo "Publisher 1:"
docker logs phase1-pipeline-publisher1-1 --tail 2 2>/dev/null | tail -2
echo "Subscriber:"
docker logs phase1-pipeline-subscriber-1 --tail 2 2>/dev/null | tail -2

echo -e "\n5. Total Records in Database:"
docker exec phase1-pipeline-mysql-1 mysql -u energy_user -penergy_pass energy_db -e "
SELECT SUM(TABLE_ROWS) as total_records 
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'energy_db';" 2>/dev/null
