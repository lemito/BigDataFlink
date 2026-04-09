SET
    'execution.runtime-mode' = 'streaming';

SET
    'execution.attached' = 'false';

SET
    'parallelism.default' = '1';

SET
    'execution.checkpointing.interval' = '10s';

CREATE TABLE
    kafka_source (
        id INT,
        customer_first_name STRING,
        customer_last_name STRING,
        customer_age INT,
        customer_email STRING,
        customer_country STRING,
        customer_postal_code STRING,
        customer_pet_type STRING,
        customer_pet_name STRING,
        customer_pet_breed STRING,
        seller_first_name STRING,
        seller_last_name STRING,
        seller_email STRING,
        seller_country STRING,
        seller_postal_code STRING,
        product_name STRING,
        product_category STRING,
        product_price FLOAT,
        product_weight FLOAT,
        product_color STRING,
        product_size STRING,
        product_brand STRING,
        product_material STRING,
        product_description STRING,
        product_rating FLOAT,
        product_reviews INT,
        product_release_date STRING,
        product_expiry_date STRING,
        pet_category STRING,
        sale_date STRING,
        sale_customer_id INT,
        sale_seller_id INT,
        sale_product_id INT,
        sale_quantity INT,
        sale_total_price FLOAT,
        store_name STRING,
        store_location STRING,
        store_city STRING,
        store_state STRING,
        store_country STRING,
        store_phone STRING,
        store_email STRING,
        supplier_name STRING,
        supplier_contact STRING,
        supplier_email STRING,
        supplier_phone STRING,
        supplier_address STRING,
        supplier_city STRING,
        supplier_country STRING
    )
WITH
    (
        'connector' = 'kafka',
        'topic' = 'csv_data',
        'properties.bootstrap.servers' = 'broker:9092',
        'properties.group.id' = 'flink-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    );

CREATE TABLE
    sink_dim_suppliers (
        name STRING,
        contact STRING,
        email STRING,
        phone STRING,
        address STRING,
        city STRING,
        country STRING
    )
WITH
    (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://db:5432/db',
        'table-name' = 'dim_suppliers',
        'username' = 'meow',
        'password' = 'UwU'
    );

CREATE TABLE
    sink_dim_pets (
        name STRING,
        breed_name STRING,
        pet_type_name STRING
    )
WITH
    (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://db:5432/db',
        'table-name' = 'dim_pets',
        'username' = 'meow',
        'password' = 'UwU'
    );

CREATE TABLE
    sink_dim_stores (
        name STRING,
        location STRING,
        city STRING,
        state STRING,
        country STRING,
        phone STRING,
        email STRING
    )
WITH
    (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://db:5432/db',
        'table-name' = 'dim_stores',
        'username' = 'meow',
        'password' = 'UwU'
    );

CREATE TABLE
    sink_dim_sellers (
        seller_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        country STRING,
        postal_code STRING,
        PRIMARY KEY (seller_id) NOT ENFORCED
    )
WITH
    (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://db:5432/db',
        'table-name' = 'dim_sellers',
        'username' = 'meow',
        'password' = 'UwU'
    );

CREATE TABLE
    sink_dim_customers (
        customer_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        age INT,
        country STRING,
        postal_code STRING,
        pet_id INT,
        PRIMARY KEY (customer_id) NOT ENFORCED
    )
WITH
    (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://db:5432/db',
        'table-name' = 'dim_customers',
        'username' = 'meow',
        'password' = 'UwU'
    );

CREATE TABLE
    sink_dim_products (
        product_id INT,
        name STRING,
        pet_category STRING,
        category STRING,
        price DECIMAL(10, 2),
        weight DECIMAL(10, 2),
        color STRING,
        size STRING,
        brand STRING,
        material STRING,
        description STRING,
        rating DECIMAL(3, 1),
        reviews STRING,
        release_date DATE,
        expiry_date DATE,
        supplier_id INT,
        PRIMARY KEY (product_id) NOT ENFORCED
    )
WITH
    (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://db:5432/db',
        'table-name' = 'dim_products',
        'username' = 'meow',
        'password' = 'UwU'
    );

CREATE TABLE
    sink_fact_sales (
        customer_id INT,
        seller_id INT,
        product_id INT,
        store_id INT,
        quantity INT,
        total_price DECIMAL(10, 2),
        `date` DATE
    )
WITH
    (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://db:5432/db',
        'table-name' = 'fact_sales',
        'username' = 'meow',
        'password' = 'UwU'
    );

BEGIN STATEMENT
SET;

INSERT INTO
    sink_dim_suppliers
SELECT DISTINCT
    supplier_name AS name,
    supplier_contact AS contact,
    supplier_email AS email,
    supplier_phone AS phone,
    supplier_address AS address,
    supplier_city AS city,
    supplier_country AS country
FROM
    kafka_source
WHERE
    supplier_email IS NOT NULL;

INSERT INTO
    sink_dim_pets
SELECT DISTINCT
    customer_pet_name AS name,
    customer_pet_breed AS breed_name,
    customer_pet_type AS pet_type_name
FROM
    kafka_source
WHERE
    customer_pet_name IS NOT NULL;

INSERT INTO
    sink_dim_stores
SELECT DISTINCT
    store_name AS name,
    store_location AS location,
    store_city AS city,
    store_state AS state,
    store_country AS country,
    store_phone AS phone,
    store_email AS email
FROM
    kafka_source
WHERE
    store_email IS NOT NULL;

INSERT INTO
    sink_dim_sellers
SELECT DISTINCT
    sale_seller_id AS seller_id,
    seller_first_name AS first_name,
    seller_last_name AS last_name,
    seller_email AS email,
    seller_country AS country,
    seller_postal_code AS postal_code
FROM
    kafka_source
WHERE
    sale_seller_id IS NOT NULL;

INSERT INTO
    sink_dim_customers
SELECT DISTINCT
    sale_customer_id AS customer_id,
    customer_first_name AS first_name,
    customer_last_name AS last_name,
    customer_email AS email,
    customer_age AS age,
    customer_country AS country,
    customer_postal_code AS postal_code,
    CAST(
        CRC32 (customer_email || customer_pet_name) AS INT
    ) AS pet_id
FROM
    kafka_source
WHERE
    sale_customer_id IS NOT NULL;

INSERT INTO
    sink_dim_products
SELECT DISTINCT
    sale_product_id AS product_id,
    product_name AS name,
    pet_category,
    product_category AS category,
    CAST(product_price AS DECIMAL(10, 2)) AS price,
    CAST(product_weight AS DECIMAL(10, 2)) AS weight,
    product_color AS color,
    product_size AS size,
    product_brand AS brand,
    product_material AS material,
    product_description AS description,
    CAST(product_rating AS DECIMAL(3, 1)) AS rating,
    CAST(product_reviews AS STRING) AS reviews,
    TO_DATE (product_release_date, 'M/d/yyyy') AS release_date,
    TO_DATE (product_expiry_date, 'M/d/yyyy') AS expiry_date,
    CAST(CRC32 (supplier_email) AS INT) AS supplier_id
FROM
    kafka_source
WHERE
    sale_product_id IS NOT NULL;

INSERT INTO
    sink_fact_sales
SELECT
    sale_customer_id AS customer_id,
    sale_seller_id AS seller_id,
    sale_product_id AS product_id,
    CAST(CRC32 (store_email) AS INT) AS store_id,
    sale_quantity AS quantity,
    CAST(sale_total_price AS DECIMAL(10, 2)) AS total_price,
    CAST(TO_TIMESTAMP (sale_date, 'M/d/yyyy') AS DATE) AS `date`
FROM
    kafka_source
WHERE
    sale_customer_id IS NOT NULL;

END;
