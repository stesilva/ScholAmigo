from kafka import KafkaProducer
import json
import time
from event_generator import click_event_generator
import random
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPICS = {
    'search': 'search_events',
    'filter': 'filter_events',
    'details': 'details_events',
    'faq': 'faq_events'
}

class KafkaClickEventProducer:
    def __init__(self, bootstrap_servers, topics):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        self.topics = topics
        self.click_event_generator = click_event_generator.ClickEventGenerator()

    def send_event(self, event_type):
        try:
            if event_type == 'search':
                event = self.click_event_generator.generate_search_event()
            elif event_type == 'filter':
                event = self.click_event_generator.generate_filter_event()
            elif event_type == 'details':
                event = self.click_event_generator.generate_details_event()
            elif event_type == 'faq':
                event = self.click_event_generator.generate_faq_event()

            future = self.producer.send(self.topics[event_type], key=event['User_ID'], value=event)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"{event_type.capitalize()} event sent: Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
        except Exception as e:
            logger.error(f"Error sending {event_type} event: {repr(e)}")

    def close(self):
        self.producer.flush()
        self.producer.close()

def main():
    producer = KafkaClickEventProducer(BOOTSTRAP_SERVERS, TOPICS)
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=5)
    
    try:
        while datetime.now() < end_time:
            event_type = random.choice(list(TOPICS.keys()))
            producer.send_event(event_type)
            time.sleep(random.uniform(1, 2))
    finally:
        producer.close()

if __name__ == "__main__":
    main()
