-- Sample data for Delta Sharing demonstration
-- This file is automatically loaded by Spring Boot on startup

-- Insert sample shares
INSERT INTO delta_shares (name, description, active, created_at, updated_at) 
VALUES ('demo-share', 'Demonstration share for Delta Sharing protocol', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO delta_shares (name, description, active, created_at, updated_at) 
VALUES ('sales-share', 'Sales data sharing', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Insert sample schemas
INSERT INTO delta_schemas (name, description, share_id, created_at, updated_at)
VALUES ('default', 'Default schema', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO delta_schemas (name, description, share_id, created_at, updated_at)
VALUES ('analytics', 'Analytics schema', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO delta_schemas (name, description, share_id, created_at, updated_at)
VALUES ('sales', 'Sales schema', 2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Insert sample tables
INSERT INTO delta_tables (name, description, schema_id, share_as_view, location, table_format, created_at, updated_at)
VALUES ('customers', 'Customer data table', 1, false, '/data/customers', 'parquet', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO delta_tables (name, description, schema_id, share_as_view, location, table_format, created_at, updated_at)
VALUES ('orders', 'Orders data table', 1, false, '/data/orders', 'parquet', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO delta_tables (name, description, schema_id, share_as_view, location, table_format, created_at, updated_at)
VALUES ('revenue', 'Revenue analytics table', 2, false, '/data/analytics/revenue', 'delta', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO delta_tables (name, description, schema_id, share_as_view, location, table_format, created_at, updated_at)
VALUES ('transactions', 'Sales transactions', 3, false, '/data/sales/transactions', 'parquet', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
