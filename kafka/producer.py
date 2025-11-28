from kafka import KafkaProducer
import json
import random
import numpy as np
from datetime import datetime
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

DATA_PATH = 'work/data/'

userIds = np.load(DATA_PATH + 'userIds.npy', allow_pickle=True)
movieIds = np.load(DATA_PATH + 'movieIds.npy', allow_pickle=True)
ratings = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

def get_random_user(userIds):
    return np.random.choice(userIds)

def get_random_movie(movieIds):
    return np.random.choice(movieIds)

def get_random_rating(ratings):
    return random.choice(ratings)

try:
    while True:
        event = {
            'user_id': int(get_random_user(userIds)),
            'movieId': int(get_random_movie(movieIds)),
            'rating': get_random_rating(ratings),
            'date': datetime.now().date().isoformat()
        }

        producer.send('user_events', event)
        producer.flush()
        
        print(f"Sent event: {event}")
        time.sleep(random.uniform(1, 3))
except KeyboardInterrupt:
    print("Stopping event generator...")
finally:
    producer.close()