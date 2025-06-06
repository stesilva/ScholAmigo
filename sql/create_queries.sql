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
CREATE TABLE IF NOT EXISTS faq_table (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    timestamp TIMESTAMP,
    page VARCHAR(100),
    clicked_element VARCHAR(100),
    clicked_parameter TEXT,
    duration FLOAT,
    location TEXT
);
CREATE TABLE IF NOT EXISTS recommendations_table (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255),
    source_scholarship_id VARCHAR(255),
    recommended_scholarship_id VARCHAR(255),
    similarity_score FLOAT,
    timestamp TIMESTAMP,
    clicked BOOLEAN DEFAULT FALSE
);