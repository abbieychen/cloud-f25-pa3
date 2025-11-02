-- Create database and user
CREATE DATABASE IF NOT EXISTS energy_db;
USE energy_db;

-- Create user for application
CREATE USER IF NOT EXISTS 'energy_user'@'%' IDENTIFIED BY 'energy_pass';
GRANT ALL PRIVILEGES ON energy_db.* TO 'energy_user'@'%';
FLUSH PRIVILEGES;

-- Shard tables will be created automatically by the application