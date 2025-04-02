#!/bin/bash
# Had to transfer the data to a 5.7 MySQL


# Step 1: Start MySQL 5.7 container
sudo docker run --name mysql57 \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -p 3307:3306 \
  -d mysql:5.7
# Step 2: Wait a few seconds for MySQL to initialize
# this is a must!!
echo "Waiting for MySQL to initialize..."
sleep 10

# Step 3: Copy schema and data into the container
sudo docker cp sakila-schema.sql mysql57:/sakila-schema.sql
sudo docker cp sakila-data.sql mysql57:/sakila-data.sql

# Step 4: Execute schema and data scripts
sudo docker exec -i mysql57 mysql -u root -prootpass < sakila-schema.sql
sudo docker exec -i mysql57 mysql -u root -prootpass sakila < sakila-data.sql

# Step 5: Load into PostgreSQL using pgloader
sudo docker run --rm dimitri/pgloader:latest \
  pgloader \
  mysql://root:rootpass@host.docker.internal:3307/sakila \
  postgresql://postgres:postgres@192.168.1.207:5432/sakila

echo "Migration completed!"

echo "🔍 Running validation script..."
##
#test commit