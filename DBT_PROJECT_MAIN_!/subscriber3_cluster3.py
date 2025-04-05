import asyncio
import json
from flask import Flask, jsonify
from kafka import KafkaConsumer, KafkaProducer
from threading import Thread

app = Flask(__name__)

SUBSCRIBE_TOPIC = "cluster3" 
PUBLISH_TOPIC = "subscriber33" 
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    SUBSCRIBE_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="subscriber33-group", 
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

latest_data = {}


def consume_kafka():
    print(f"Subscribed to Kafka topic '{SUBSCRIBE_TOPIC}'")
    global latest_data

    for message in consumer:
        data = json.loads(message.value.decode("utf-8"))
        print(f"Received from Kafka: {data}")

        latest_data = data

        publish_to_kafka(data)


def publish_to_kafka(data):
    """Publish the received data to the `subscriber33` topic."""
    try:
        producer.send(PUBLISH_TOPIC, data)
        print(f"Published to Kafka topic '{PUBLISH_TOPIC}': {data}")
    except Exception as e:
        print(f"Error publishing to Kafka: {e}")


@app.route('/get_data')
def get_data():
    """Handle long-polling requests."""
    global latest_data

    if latest_data:
        return jsonify(latest_data)
    else:
        return jsonify({"message": "No data available"}), 204


def run_flask():
    app.run(host="localhost", port=4004) 


async def main():
    kafka_thread = Thread(target=consume_kafka)
    kafka_thread.daemon = True
    kafka_thread.start()

    flask_thread = Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    while True:
        await asyncio.sleep(1)  


if __name__ == "__main__":
    asyncio.run(main())
