INSERT INTO search_table 
(user_id, timestamp, page, clicked_element, clicked_parameter, duration, location)
VALUES (%s, %s, %s, %s, %s, %s, %s);

INSERT INTO filter_table 
(user_id, timestamp, page, clicked_element, clicked_parameter, duration, location)
VALUES (%s, %s, %s, %s, %s, %s, %s);

INSERT INTO details_table 
(user_id, timestamp, page, clicked_element, clicked_parameter, duration, location)
VALUES (%s, %s, %s, %s, %s, %s, %s);

INSERT INTO faq_table 
(user_id, timestamp, page, clicked_element, clicked_parameter, duration, location)
VALUES (%s, %s, %s, %s, %s, %s, %s);
