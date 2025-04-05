from kafka import KafkaConsumer, KafkaProducer


KAFKA_TOPIC = "highestemoji"
PUBLISHER_TOPICS = ["publisher1_topic", "publisher2_topic", "publisher3_topic"]

KAFKA_BROKER = "localhost:9092" 

def create_producer():
    """Create Kafka producer."""
    return KafkaProducer(bootstrap_servers=[KAFKA_BROKER])

def forward_message_to_publishers(emoji, producer):
    """Forward the message to multiple publishers."""
    for topic in PUBLISHER_TOPICS:
        producer.send(topic, value=emoji.encode('utf-8'))
        print(f"Sent emoji: {emoji} to {topic}")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest', 
        enable_auto_commit=True,  
        group_id="emoji_consumer_group",  
    )

    producer = create_producer()

    print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")

    for message in consumer:
        emoji = message.value.decode('utf-8')
        print(f"Received emoji: {emoji}")
        forward_message_to_publishers(emoji, producer)

if __name__ == "__main__":
    main()

