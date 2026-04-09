import json
import os
import time
from kafka import KafkaConsumer

# Environment variables
TOPIC = os.environ.get('TOPIC', 'Topic1')
CONSUMER_GROUP = os.environ.get('CONSUMER_GROUP', 'lab3-group')
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', 'localhost:9091,localhost:9092').split(',')

def setup_consumer():
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        print(f'Waiting for brokers... Error: {e}')
        return None

def consume_messages():
    print(f'Starting consumer for topic {TOPIC}...')
    consumer = None
    while consumer is None:
        consumer = setup_consumer()
        if consumer is None:
            time.sleep(5)

    print('Connected to Kafka. Waiting for messages...')
    try:
        for message in consumer:
            print(f"Received trip_id: {message.value.get('trip_id')} from {message.topic}")
    except Exception as e:
        print(f'Error during consumption: {e}')
    finally:
        if consumer:
            consumer.close()

if __name__ == '__main__':
    consume_messages()
