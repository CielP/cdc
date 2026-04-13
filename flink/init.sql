CREATE TABLE products_source (
    product_id  STRING,
    sku         STRING,
    name        STRING,
    category    STRING,
    price       DECIMAL(10, 2),
    stock_qty   INT,
    is_active   BOOLEAN,
    created_at  STRING,
    updated_at  STRING,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'db_a.public.products',
    'properties.bootstrap.servers'  = 'kafka:9092',
    'properties.group.id'           = 'flink-cdc-consumer',
    'scan.startup.mode'             = 'earliest-offset',
    'format'                        = 'debezium-json'
);

CREATE TABLE products_sink (
    product_id  STRING,
    sku         STRING,
    name        STRING,
    category    STRING,
    price       DECIMAL(10, 2),
    stock_qty   INT,
    is_active   BOOLEAN,
    created_at  TIMESTAMP(3),
    updated_at  TIMESTAMP(3),
    synced_at   TIMESTAMP(3),
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector'                      = 'jdbc',
    'url'                            = 'jdbc:postgresql://db-b:5432/targetdb',
    'table-name'                     = 'products',
    'username'                       = 'postgres',
    'password'                       = 'postgres',
    'sink.buffer-flush.max-rows'     = '1',
    'sink.buffer-flush.interval'     = '0'
);

INSERT INTO products_sink
SELECT
    product_id,
    sku,
    name,
    category,
    price,
    stock_qty,
    is_active,
    TO_TIMESTAMP(REPLACE(SUBSTRING(created_at, 1, 23), 'T', ' ')) AS created_at,
    TO_TIMESTAMP(REPLACE(SUBSTRING(updated_at, 1, 23), 'T', ' ')) AS updated_at,
    LOCALTIMESTAMP AS synced_at
FROM products_source;
