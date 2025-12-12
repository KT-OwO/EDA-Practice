import os
import time
import json
import random
from kafka import KafkaProducer
import psycopg2

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
PG_HOST = os.getenv("POSTGRES_HOST", "eventdb")
PG_DB = os.getenv("POSTGRES_DB", "events")
PG_USER = os.getenv("POSTGRES_USER", "event_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "event_pass")

producer = KafkaProducer(bootstrap_servers=[KAFKA],
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# simple function to insert an event into Postgres (simulate event source)
def insert_event_to_db(payload):
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS produced_events (
            id SERIAL PRIMARY KEY,
            topic TEXT,
            payload JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("INSERT INTO produced_events (topic, payload) VALUES (%s, %s);",
                ('events', json.dumps(payload)))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    i = 0
    while True:
        event = {
            "id": i,
            "type": "order.created",
            "amount": round(random.random() * 100, 2)
        }
        # write to DB (simulate event source DB)
        try:
            insert_event_to_db(event)
            print("Inserted event to DB:", event)
        except Exception as e:
            print("DB insert failed:", e)

        # publish to Kafka
        try:
            producer.send("events", event)
            producer.flush()
            print("Produced:", event)
        except Exception as e:
            print("Kafka produce failed:", e)

        i += 1
        time.sleep(3)
