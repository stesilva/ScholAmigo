from kafka import KafkaProducer
import json
import time
import sysnthetic_data
import random
import datetime

# Kafka configuration
KAFKA_TOPIC = 'users_events'
BOOTSTRAP_SERVERS = ['localhost:9092']  # Replace with your Kafka broker address

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Usage example:
data_generator = sysnthetic_data.generate_synthetic_data()
start_time = datetime.datetime.now()
duration = datetime.timedelta(minutes=2)

try:
    while datetime.datetime.now() - start_time < duration:
        data = next(data_generator)
        producer.send(KAFKA_TOPIC, data)
        print(f"Sent: {data}")
        time.sleep(random.uniform(0.1, 1.0))
finally:
    producer.close()
    print("Producer stopped after 2 minutes.")
