-- Supplier dimension table
CREATE TABLE
    dim_suppliers (
        supplier_id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE, -- Требуется для UPSERT во Flink
        contact VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(50),
        address TEXT,
        city VARCHAR(100),
        country VARCHAR(100)
    );
    
    -- Pets dimension table
    CREATE TABLE
        dim_pets (
        pet_id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        breed_name VARCHAR(255),
        pet_type_name VARCHAR(255),
        UNIQUE (name, breed_name, pet_type_name) -- Требуется для UPSERT
    );
    
    -- Stores dimension table
    CREATE TABLE
        dim_stores (
        name VARCHAR(255) PRIMARY KEY,
        location VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(100),
        country VARCHAR(100),
        phone VARCHAR(50),
        email VARCHAR(255)
    );
    
    -- Sellers dimension table
    CREATE TABLE
        dim_sellers (
        seller_id INTEGER PRIMARY KEY, -- Берем из Kafka
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        email VARCHAR(255),
        country VARCHAR(100),
        postal_code VARCHAR(20)
    );
    
    -- Customers dimension table
    CREATE TABLE
        dim_customers (
        customer_id INTEGER PRIMARY KEY, -- Берем из Kafka
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        email VARCHAR(255),
        age INTEGER,
        country VARCHAR(100),
        postal_code VARCHAR(20),
        pet_id INTEGER
    );
    
    -- Products dimension table
    CREATE TABLE
        dim_products (
        product_id INTEGER PRIMARY KEY, -- Берем из Kafka
        name VARCHAR(255),
        pet_category VARCHAR(100),
        category VARCHAR(100),
        price DECIMAL(10, 2),
        weight DECIMAL(10, 2),
        color VARCHAR(50),
        size VARCHAR(50),
        brand VARCHAR(100),
        material VARCHAR(100),
        description TEXT,
        rating DECIMAL(3, 1),
        reviews INTEGER, -- Исправлено с TEXT на INTEGER
        release_date DATE,
        expiry_date DATE,
        supplier_id INTEGER
    );
    
    -- Fact sales table
    CREATE TABLE
        fact_sales (
        sale_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        seller_id INTEGER,
        product_id INTEGER,
        store_name VARCHAR(255),
        supplier_name VARCHAR(255),
        quantity INTEGER,
        total_price DECIMAL(10, 2),
        sale_date DATE
    );
