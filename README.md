# CDC Pipeline: PostgreSQL → Debezium → Kafka → Flink → PostgreSQL

A local Change Data Capture (CDC) pipeline that automatically replicates changes from one PostgreSQL database to another using Debezium, Kafka, and Flink. All services run via Docker Compose on a single machine.

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

| Service | Image | Purpose |
|---|---|---|
| **DB-A** | postgres:16 + pg_uuidv7 | Source database with `products` table |
| **DB-B** | postgres:16 + pg_uuidv7 | Target database receiving replicated data |
| **Kafka** | apache/kafka:3.7.0 | Message broker (KRaft mode, no ZooKeeper) |
| **Debezium** | debezium/connect:2.7.3.Final | Captures WAL changes from DB-A into Kafka |
| **Flink** | flink:1.19-java11 | Reads Kafka topic and writes to DB-B via JDBC |
| **pgAdmin** | dpage/pgadmin4 | Web UI for inspecting both databases |

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- ~2 GB of free memory for all containers
- `curl` (for downloading Flink JARs)

## Quick Start

### 1. Download Flink JAR dependencies

```bash
mkdir -p flink/lib && cd flink/lib

curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar
curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar
curl -LO https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

cd ../..
```

### 2. Start all services

```bash
docker compose up -d
```

Wait for all containers to become healthy. The `connector-init` service will automatically register the Debezium source connector once Kafka Connect is ready.

### 3. Submit the Flink SQL job

```bash
docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/usrlib/init.sql
```

### 4. Test the pipeline

Insert a row into DB-A:

```bash
docker exec -it db-a psql -U postgres -d sourcedb -c \
  "INSERT INTO products (sku, name, category, price, stock_qty) VALUES ('TEST-001', 'Test Product', 'test', 9.99, 100);"
```

Verify it appears in DB-B:

```bash
docker exec -it db-b psql -U postgres -d targetdb -c "SELECT * FROM products;"
```

## Web UIs

| UI | URL | Credentials |
|---|---|---|
| Flink Dashboard | http://localhost:8081 | — |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |

## Exposed Ports

| Port | Service |
|---|---|
| 5432 | DB-A (PostgreSQL source) |
| 5433 | DB-B (PostgreSQL target) |
| 8081 | Flink Web UI |
| 8083 | Kafka Connect REST API |
| 9094 | Kafka (external listener) |
| 5050 | pgAdmin |

## Project Structure

```
├── CLAUDE.md                       # AI assistant guidance
├── docker-compose.yml              # All service definitions
├── debezium/
│   └── register-connector.sh       # Debezium connector registration script
├── flink/
│   ├── init.sql                    # Flink SQL job (source → sink)
│   └── lib/                        # Flink JAR dependencies (git-ignored)
├── pgadmin/
│   └── servers.json                # Pre-configured pgAdmin server list
├── postgres/
│   ├── Dockerfile                  # PostgreSQL 16 + pg_uuidv7 extension
│   ├── db-a-init.sql               # Source DB schema (products table)
│   └── db-b-init.sql               # Target DB schema (products table)
└── openspec/                       # OpenSpec workflow configuration
    └── config.yaml
```

## Useful Commands

```bash
# Check Debezium connector status
curl http://localhost:8083/connectors/products-source-connector/status

# Check running Flink jobs
curl http://localhost:8081/jobs

# Tail Kafka messages for the products topic
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic db_a.public.products \
  --from-beginning

# Rebuild after changing Dockerfile or init SQL files
docker compose up -d --build

# Full teardown (removes all data volumes)
docker compose down -v
```

## License

This project is for local development and learning purposes.
