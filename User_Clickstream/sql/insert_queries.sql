-- Insert data into search_table
INSERT INTO search_table 
(user_id, timestamp, page, clicked_element, clicked_parameter, duration, location)
VALUES (%s, %s, %s, %s, %s, %s, %s);

-- Insert data into filter_table
INSERT INTO filter_table 
(user_id, timestamp, page, clicked_element, clicked_parameter, duration, location)
VALUES (%s, %s, %s, %s, %s, %s, %s);

-- Insert data into details_table
INSERT INTO details_table 
(user_id, timestamp, page, clicked_element, clicked_parameter, duration, location)
VALUES (%s, %s, %s, %s, %s, %s, %s);

-- Insert data into faq_table
INSERT INTO faq_table 
(user_id, timestamp, page, clicked_element, clicked_parameter, duration, location)
VALUES (%s, %s, %s, %s, %s, %s, %s);
