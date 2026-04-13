#!/bin/sh

CONNECT_URL="http://kafka-connect:8083"
MAX_RETRIES=20
RETRY_INTERVAL=5

echo "Waiting for Kafka Connect to be fully ready..."
i=0
until curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge "$MAX_RETRIES" ]; then
    echo "Kafka Connect did not become ready after $((MAX_RETRIES * RETRY_INTERVAL))s. Aborting."
    exit 1
  fi
  echo "  Not ready yet, retrying in ${RETRY_INTERVAL}s... (${i}/${MAX_RETRIES})"
  sleep "$RETRY_INTERVAL"
done

echo "Kafka Connect is ready. Registering Debezium PostgreSQL source connector..."

i=0
while true; do
  RESPONSE=$(curl -s -o /tmp/response.txt -w "%{http_code}" \
    -X POST "${CONNECT_URL}/connectors" \
    -H 'Content-Type: application/json' \
    -d '{
      "name": "products-source-connector",
      "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "db-a",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "sourcedb",
        "topic.prefix": "db_a",
        "table.include.list": "public.products",
        "plugin.name": "pgoutput",
        "message.key.columns": "public.products:product_id",
        "slot.name": "debezium_products",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false"
      }
    }')

  if [ "$RESPONSE" = "201" ]; then
    cat /tmp/response.txt && echo ""
    echo "Connector registered successfully (HTTP 201)."
    exit 0
  elif [ "$RESPONSE" = "409" ]; then
    echo "Connector already exists (HTTP 409), skipping."
    exit 0
  fi

  i=$((i + 1))
  if [ "$i" -ge "$MAX_RETRIES" ]; then
    cat /tmp/response.txt && echo ""
    echo "Failed to register connector after $MAX_RETRIES attempts (last HTTP: $RESPONSE)."
    exit 1
  fi

  echo "  HTTP $RESPONSE - worker not ready yet, retrying in ${RETRY_INTERVAL}s... (${i}/${MAX_RETRIES})"
  sleep "$RETRY_INTERVAL"
done
