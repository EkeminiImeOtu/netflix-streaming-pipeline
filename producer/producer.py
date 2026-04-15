from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

NETFLIX_SHOWS = [
    "Stranger Things", "The Crown", "Ozark", "Squid Game",
    "Money Heist", "Dark", "Bridgerton", "Wednesday",
    "The Witcher", "Emily in Paris", "Narcos", "Lupin"
]

DEVICES = ["mobile", "tablet", "smart_tv", "laptop", "desktop"]
COUNTRIES = ["Nigeria", "USA", "UK", "Germany", "France", "Brazil", "India"]

def generate_viewing_event():
    return {
        "user_id": fake.uuid4(),
        "show_name": random.choice(NETFLIX_SHOWS),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "watch_duration_mins": random.randint(1, 120),
        "rating": round(random.uniform(1.0, 5.0), 1),
        "timestamp": datetime.now().isoformat(),
        "is_completed": random.choice([True, False])
    }

print("Starting Netflix viewing event producer...")
print("Sending events to topic: netflix-views")
print("Press Ctrl+C to stop\n")

count = 0
while True:
    event = generate_viewing_event()
    producer.send('netflix-views', value=event)
    count += 1
    print(f"Event {count}: {event['user_id'][:8]}... watched '{event['show_name']}' on {event['device']} from {event['country']}")
    time.sleep(1)
