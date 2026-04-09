import csv
import json
import os
import time
from kafka import KafkaProducer

# Environment variables
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', 'localhost:9091,localhost:9092').split(',')
CSV_FILE_PATH = os.environ.get('CSV_FILE_PATH', '/app/data/Divvy_Trips_2019_Q4.csv')
TOPICS = ['Topic1', 'Topic2']

def setup_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=5
        )
        return producer
    except Exception as e:
        print(f'Waiting for brokers... Error: {e}')
        return None

def produce_messages():
    print('Setting up producer...')
    producer = None
    while producer is None:
        producer = setup_producer()
        if producer is None:
            time.sleep(5)

    print(f'Loading data from {CSV_FILE_PATH}...')
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: File {CSV_FILE_PATH} not found!")
        return

    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            counter = 0
            for row in reader:
                counter += 1
                # Send to both topics
                for topic in TOPICS:
                    producer.send(topic, row)
                
                if counter % 100 == 0:
                    print(f'Sent {counter} messages to topics {TOPICS}')
                
                # Small delay to prevent overwhelming
                time.sleep(0.1) 
                
            producer.flush()
            print(f'Finished producing {counter} messages.')

    except Exception as e:
        print(f'Error during production: {e}')
    finally:
        if producer:
            producer.close()

if __name__ == '__main__':
    # Give some time for Kafka to fully initialize
    time.sleep(10)
    produce_messages()
