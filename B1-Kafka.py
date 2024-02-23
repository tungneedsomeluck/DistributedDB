from confluent_kafka import Producer
from time import sleep
import pandas as pd
import random

# Config producer
producer_config = {
    'bootstrap.servers': '127.0.0.1:9092',
    'client.id': 'python-producer'
}

# Create producer
producer = Producer(producer_config)

# Topic
topic = 'credit_card_transactions'

# Read data from CSV file
csv_file = 'User0_credit_card_transactions.csv'
df = pd.read_csv(csv_file)

# Send a message to the topic
while True:
    for index, row in df.iterrows():
        tran = row.to_json()

        # Send message
        producer.produce(topic, key=str(index), value=tran)
        producer.flush()
        sleep(1)

        print(f'Sent: {tran}')

    # Sleep for a random amount of time
    sleep(random.randint(1, 3))