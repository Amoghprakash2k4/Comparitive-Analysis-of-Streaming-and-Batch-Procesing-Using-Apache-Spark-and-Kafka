from kafka import KafkaConsumer, KafkaProducer
import json
import sys

INPUT_TOPIC = "publisher3_topic"  
FORWARD_TOPIC = "cluster3" 
KAFKA_BROKER = "localhost:9092"

def main():
    try:
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest', 
            enable_auto_commit=True, 
            group_id="emoji_forwarder_group",  
        )
        print(f"Listening for messages on topic '{INPUT_TOPIC}'...")
    except Exception as e:
        print(f"Error while connecting to Kafka: {e}")
        sys.exit(1)

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
    except Exception as e:
        print(f"Error while connecting to Kafka Producer: {e}")
        sys.exit(1)

    try:
        for message in consumer:
            try:
                emoji = json.loads(message.value.decode('utf-8'))
                print(f"Received from {INPUT_TOPIC}: {emoji}")

                producer.send(FORWARD_TOPIC, emoji)
                print(f"Forwarded to {FORWARD_TOPIC}: {emoji}")

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON message: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()

