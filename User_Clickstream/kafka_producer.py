from kafka import KafkaProducer
import json
import time
from event_generator import click_event_generator
import random

# Kafka configuration
KAFKA_TOPIC = 'users_click_events'
BOOTSTRAP_SERVERS = ['localhost:9092']

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') 
)

# Initialize click event class
clickevent_generator = click_event_generator.ClickEventGenerator()

def generate_send_event():
    try:
        click_event = clickevent_generator.generate_click_event()
        future = producer.send(KAFKA_TOPIC, key=click_event['User_ID'], value=click_event)
        metadata = future.get(timeout=5)
        print(f"Click event sent: {click_event}, Partition: {metadata.partition}, Offset: {metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {repr(e)}")    

if __name__ == "__main__":
    try:
        while True:
                generate_send_event()
                time.sleep(random.uniform(5,6)) # Choose random intervals
    finally:
        producer.close()