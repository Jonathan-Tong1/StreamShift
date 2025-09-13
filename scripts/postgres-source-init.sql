-- scripts/postgres-source-init.sql
-- Simple PostgreSQL setup for testing CDC

-- Create schema
CREATE SCHEMA IF NOT EXISTS inventory;
SET search_path = inventory, public;

-- Create test table
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO customers (first_name, last_name, email) VALUES
('John', 'Doe', 'john.doe@email.com'),
('Jane', 'Smith', 'jane.smith@email.com'),
('Bob', 'Johnson', 'bob.johnson@email.com');

-- Enable CDC
ALTER TABLE customers REPLICA IDENTITY FULL;
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Grant permissions
GRANT USAGE ON SCHEMA inventory TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO postgres;

-- Show what we created
SELECT 'customers' as table_name, COUNT(*) as row_count FROM customers;