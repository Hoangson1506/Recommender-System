from kafka import KafkaConsumer
import redis
import json
import sys

print("Starting Redis Consumer...")

try:
    consumer = KafkaConsumer(
        'user_events',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='latest',
        group_id='redis_consumers',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    redis_client.ping()

    print("Kafka and Redis connections established. Waiting for messages...")

    for message in consumer:
        event = message.value
        try:
            user_id = event['user_id']
            movie_id = event['movieId']

            redis_client.set("LATEST_RATING_EVENT", json.dumps(event))

            seen_key = f"user_seen:{user_id}"
            redis_client.sadd(seen_key, str(movie_id))

            redis_client.expire(seen_key, 86400)

            print(f"Cached user_seen:{user_id} -> {movie_id}")

        except KeyError:
            print(f"Ignoring malformed message (missing key): {event}")
        except Exception as e:
            print(f"Error processing message: {e}")

except Exception as e:
    print(f"CRITICAL ERROR: Failed to connect to Kafka or Redis. Exiting. Error: {e}")
    sys.exit(1)