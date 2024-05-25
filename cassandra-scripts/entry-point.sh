# check_cassandra_startup.sh
#!/bin/bash

CONTAINER_NAME="cassandradb"
LOG_MESSAGE="CassandraDaemon.java:761 - Startup complete"

echo "Waiting for Cassandra to start..."

while true; do
  if docker logs $CONTAINER_NAME 2>&1 | grep -q "$LOG_MESSAGE"; then
    echo "Cassandra has started!"
    sleep 5
    break
  else
    echo "Cassandra not started yet. Checking again in 10 seconds..."
    sleep 10
  fi
done

docker exec -it cassandradb cqlsh -f scripts/create-keyspace-and-tables.cql

echo "Tables created successfully!:=)"

