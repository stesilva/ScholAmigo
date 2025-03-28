from kafka import KafkaConsumer
import json
from psycopg2 import sql
import psycopg2 
import logging
from datetime import datetime, timedelta
from dateutil import parser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPICS = ['search_events', 'filter_events', 'details_events', 'faq_events']

PG_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

class KafkaClickEventConsumer:
    def __init__(self, bootstrap_servers, topics, pg_config):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8'),
            auto_offset_reset='earliest',
            group_id='click_event_consumer_group'
        )
        self.pg_conn = psycopg2.connect(**pg_config)
        self.pg_cursor = self.pg_conn.cursor()
        self.sql_create_queries = self.load_sql_queries('sql/create_queries.sql')
        self.sql_insert_queries = self.load_sql_queries('sql/insert_queries.sql')


    def load_sql_queries(self, filename):
        with open(filename, 'r') as file:
            return file.read().split(';')

    def create_tables(self):
        print(self.sql_create_queries)
        for query in self.sql_create_queries:
            print("query:",query)
            self.pg_cursor.execute(query)
        self.pg_conn.commit()

    def process_event(self, event, topic):
        table_name = topic.replace('_events', '_table')
        query_index = {'search_table': 0, 'filter_table': 1, 'details_table': 2, 'faq_table': 3}
        insert_query = self.sql_insert_queries[query_index[table_name]]
        
        self.pg_cursor.execute(sql.SQL(insert_query).format(sql.Identifier(table_name)), (
            event['User_ID'],
            parser.parse(event['Timestamp']),
            event['Page'],
            event['Clicked_Element'],
            event['Clicked_Parameter'],
            event['Duration'],
            event['Location']
        ))
        self.pg_conn.commit()

    def consume_events(self, duration_minutes):
        self.create_tables()
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)

        try:
            for message in self.consumer:
                if datetime.now() >= end_time:
                    break
                topic = message.topic
                self.process_event(message.value, topic)
                logger.info(f"Processed event from {topic}: {message.value['User_ID']}")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
        self.pg_cursor.close()
        self.pg_conn.close()

def main():
    consumer = KafkaClickEventConsumer(BOOTSTRAP_SERVERS, TOPICS, PG_CONFIG)
    consumer.consume_events(duration_minutes=5)

if __name__ == "__main__":
    main()
