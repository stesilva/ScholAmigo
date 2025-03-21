from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_TOPIC = 'users_events'
BOOTSTRAP_SERVERS = ['localhost:9092']  # Replace with your Kafka broker address

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='scholarship_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume messages
for message in consumer:
    print(f"Received: {message.value}")
