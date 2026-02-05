-- Sample data for testing
-- Users
INSERT INTO analytics.users (user_id, email) VALUES
    (1, 'user1@example.com'),
    (2, 'user2@example.com'),
    (3, 'user3@example.com');

-- Events
INSERT INTO analytics.events (user_id, event_type) VALUES
    (1, 'page_view'),
    (1, 'click'),
    (2, 'page_view'),
    (3, 'purchase');

-- System config
INSERT INTO analytics.system_config (config_key, config_value, description) VALUES
    ('app_name', 'Airflow Analytics', 'Application name'),
    ('environment', 'development', 'Current environment');
