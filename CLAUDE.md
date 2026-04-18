# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A local CDC (Change Data Capture) pipeline: **DB-A (PostgreSQL source)** → **Debezium** → **Kafka** → **Flink** → **DB-B (PostgreSQL target)**. Changes to the `products` table in DB-A are automatically synced to DB-B. All services run via Docker Compose on a single machine.

## Common Commands

```bash
# Start all services
docker compose up -d

# Rebuild and start (after changing postgres/Dockerfile or SQL init files)
docker compose up -d --build

# Tear down and remove all volumes (full reset)
docker compose down -v

# Check connector status
curl http://localhost:8083/connectors/products-source-connector/status

# Submit Flink SQL job (run after all services are healthy)
docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/usrlib/init.sql

# Check running Flink jobs
curl http://localhost:8081/jobs

# Flink web UI
open http://localhost:8081

# pgAdmin web UI (admin@admin.com / admin)
open http://localhost:5050

# Tail Kafka messages for the products topic
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic db_a.public.products \
  --from-beginning
```

## Architecture

```
DB-A (sourcedb:5432) ──WAL──► Debezium (kafka-connect:8083)
                                        │
                                   Kafka (kafka:9092)
                                   topic: db_a.public.products
                                        │
                               Flink (jobmanager:8081)
                               SQL job: flink/init.sql
                                        │
                              DB-B (targetdb:5433)
```

- **DB-A** requires `wal_level=logical` and `REPLICA IDENTITY FULL` on the `products` table (already set in init.sql). The `pg_uuidv7` extension provides `uuid_generate_v7()` for the primary key.
- **DB-B** `product_id` column is `TEXT` (not `UUID`) because Flink JDBC maps STRING→VARCHAR and PostgreSQL prepared statements do not coerce VARCHAR→UUID implicitly.
- **Kafka** runs in KRaft mode (no ZooKeeper). The `kafka-init` service pre-creates the `__consumer_offsets` topic; this is required because `apache/kafka:3.7.0` in KRaft mode does not auto-create it reliably.
- **Flink** reads from Kafka in `debezium-json` format. Timestamps arrive as ISO 8601 strings (`2026-04-13T07:33:17.649401Z`); they are converted via `TO_TIMESTAMP(REPLACE(SUBSTRING(col, 1, 23), 'T', ' '))` because Flink's debezium-json format expects `TIMESTAMP(3)` in `yyyy-MM-dd HH:mm:ss.SSS` format. `TIMESTAMP_LTZ` is not supported by the JDBC sink connector.
- **Debezium connector** is registered automatically by the `connector-init` service using `debezium/register-connector.sh`. Key settings: `decimal.handling.mode=string`, `schemas.enable=false`.

## Full Reset / Re-snapshot Procedure

When Kafka messages are corrupted or connector config changes require a fresh snapshot:

```bash
# 1. Cancel the Flink job via web UI or API
# 2. Delete the connector
curl -X DELETE http://localhost:8083/connectors/products-source-connector

# 3. Delete the Kafka topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic db_a.public.products

# 4. Restart kafka-connect to release the replication slot
docker compose restart kafka-connect

# 5. Drop the replication slot in DB-A
docker exec -it db-a psql -U postgres -d sourcedb \
  -c "SELECT pg_drop_replication_slot('debezium_products');"

# 6. Re-register the connector (connector-init runs once; do it manually)
docker exec connector-init sh /register-connector.sh

# 7. Resubmit the Flink job
docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/usrlib/init.sql
```

## Flink JAR Setup

JAR files are excluded from the repository. Download them before starting:

```bash
mkdir -p flink/lib
cd flink/lib

# Flink Kafka SQL connector
curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar

# Flink JDBC connector
curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar

# PostgreSQL JDBC driver
curl -LO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
```

## Known Gotchas

- **Flink sql-client double-quote bug**: Double quotes inside `--` comments (e.g., `-- e.g. "value"`) confuse the statement splitter and silently drop the `INSERT` statement. Keep `flink/init.sql` free of double quotes and non-ASCII characters.
- **kafka-init ordering**: `kafka-connect` depends on `kafka-init: condition: service_completed_successfully`. Do not remove this; without it `__consumer_offsets` may not exist and all connector registrations return HTTP 500.
- **REPLICA IDENTITY FULL**: Required on `products` in DB-A. Without it, Debezium UPDATE events have `before=null`, which causes Flink's debezium-json format to throw an error.
- **Flink JAR files** are in `flink/lib/` and are mounted into both `flink-jobmanager` and `flink-taskmanager`. The versions are pinned: `flink-sql-connector-kafka-3.3.0-1.19.jar`, `flink-connector-jdbc-3.2.0-1.19.jar`, `postgresql-42.7.3.jar`.
