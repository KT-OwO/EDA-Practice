import os
import json
from kafka import KafkaConsumer
import psycopg2

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
PG_HOST = os.getenv("POSTGRES_HOST", "eventdb")
PG_DB = os.getenv("POSTGRES_DB", "events")
PG_USER = os.getenv("POSTGRES_USER", "event_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "event_pass")

consumer = KafkaConsumer('events',
                        bootstrap_servers=[KAFKA],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        value_deserializer=lambda m: json.loads(m.decode('utf-8')))

def save_handled_event(ev):
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS consumed_events (
            id SERIAL PRIMARY KEY,
            event_id INT,
            event_type TEXT,
            payload JSONB,
            consumed_at TIMESTAMP DEFAULT NOW()
        );
    """)
    cur.execute("INSERT INTO consumed_events (event_id, event_type, payload) VALUES (%s, %s, %s);",
                (ev.get('id'), ev.get('type'), json.dumps(ev)))
    conn.commit()
    cur.close()
    conn.close()

print("Consumer started, waiting for messages...")
for msg in consumer:
    event = msg.value
    print("Received:", event)
    try:
        save_handled_event(event)
        print("Saved consumed event to DB")
    except Exception as e:
        print("Failed to save consumed event:", e)
