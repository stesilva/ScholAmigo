-- Create table for search events
CREATE TABLE IF NOT EXISTS search_table (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    timestamp TIMESTAMP,
    page VARCHAR(100),
    clicked_element VARCHAR(100),
    clicked_parameter TEXT,
    duration FLOAT,
    location TEXT
);
-- Create table for filter events
CREATE TABLE IF NOT EXISTS filter_table (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    timestamp TIMESTAMP,
    page VARCHAR(100),
    clicked_element VARCHAR(100),
    clicked_parameter TEXT,
    duration FLOAT,
    location TEXT
);
-- Create table for details events
CREATE TABLE IF NOT EXISTS details_table (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    timestamp TIMESTAMP,
    page VARCHAR(100),
    clicked_element VARCHAR(100),
    clicked_parameter TEXT,
    duration FLOAT,
    location TEXT
);
-- Create table for faq events
CREATE TABLE IF NOT EXISTS faq_table (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    timestamp TIMESTAMP,
    page VARCHAR(100),
    clicked_element VARCHAR(100),
    clicked_parameter TEXT,
    duration FLOAT,
    location TEXT
)