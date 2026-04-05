CREATE TABLE dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    contact VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE dim_pets (
    pet_id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    breed_name VARCHAR(255),
    pet_type_name VARCHAR(255)
);

CREATE TABLE dim_stores (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(255) UNIQUE
);

CREATE TABLE dim_sellers (
    seller_id INTEGER PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    age INTEGER,
    country VARCHAR(100),
    postal_code VARCHAR(20),
    pet_id INTEGER REFERENCES dim_pets(pet_id)
);

CREATE TABLE dim_products (
    product_id INTEGER PRIMARY KEY,
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
    reviews TEXT,
    release_date DATE,
    expiry_date DATE,
    supplier_id INTEGER REFERENCES dim_suppliers(supplier_id),
    supplier_name VARCHAR(255),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    seller_id INTEGER REFERENCES dim_sellers(seller_id),
    product_id INTEGER REFERENCES dim_products(product_id),
    store_id INTEGER REFERENCES dim_stores(store_id),
    quantity INTEGER,
    total_price DECIMAL(10, 2),
    date DATE
);
