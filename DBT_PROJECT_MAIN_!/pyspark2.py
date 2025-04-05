from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, lit
from pyspark.sql.types import StructType, StringType
from kafka import KafkaProducer
import json
import mysql.connector
import time
import psutil

# Define schema
schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji_type", StringType()) \
    .add("timestamp", StringType())

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EmojiAggregatorEvery2Sec") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka producer to send top emoji to 'highestemoji' topic
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka stream source
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emojiaggregator") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# MySQL connection config
mysql_config = {
    "host": "localhost",
    "user": "amogh",
    "password": "yourpassword",  # Replace with actual password
    "database": "emostream"
}

# Current process for resource usage
process = psutil.Process()

def process_batch(batch_df, batch_id):
    print(f"\n\U0001f4e6 Starting processing for Batch ID: {batch_id}")
    start_time = time.time()

    # CPU & memory BEFORE processing
    cpu_before = process.cpu_percent(interval=None)
    mem_before = process.memory_info().rss / 1024**2  # MB

    total_rows = batch_df.count()
    print(f"Batch {batch_id} contains {total_rows} rows")
    if total_rows == 0:
        print("No data in this batch.")
        return

    emoji_counts = batch_df.groupBy("emoji_type").count()

    total_emoji_count = emoji_counts.agg({"count": "sum"}).collect()[0][0]
    normalized_emoji_counts = emoji_counts.withColumn(
        "normalized_count", col("count") / lit(total_emoji_count)
    )

    emoji_results = normalized_emoji_counts.collect()

    # Send most used emoji to Kafka
    filtered_emojis = [row for row in emoji_results if row['normalized_count'] >= 0.26]

    if len(filtered_emojis) == 0:
        most_used_emoji = "NONE"
        print(f"Most used emoji: {most_used_emoji}\n")
    else:
        most_used_emoji = max(filtered_emojis, key=lambda x: x['count'])
        emoji_to_send = most_used_emoji['emoji_type']
        print(f"Most used emoji: {emoji_to_send} (Count: {most_used_emoji['count']}, Normalized: {most_used_emoji['normalized_count']:.2f})\n")
        producer.send('highestemoji', {'emoji_type': emoji_to_send})

    # Insert aggregated data into MySQL
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()

        for row in emoji_results:
            query = """
                INSERT INTO aggregated_emoji_data (batch_id, emoji_type, count, normalized_count)
                VALUES (%s, %s, %s, %s)
            """
            try:
                cursor.execute(query, (
                    batch_id,
                    row['emoji_type'],
                    int(row['count']),
                    float(row['normalized_count'])
                ))
            except mysql.connector.IntegrityError as dup_err:
                print(f"\u274c Duplicate entry error: {dup_err}")

        connection.commit()
        cursor.close()
        connection.close()
        print("\u2705 Aggregated data inserted into MySQL for batch", batch_id)

    except mysql.connector.Error as err:
        print("\u274c MySQL error:", err)

    # Store raw data for batch mode
    try:
        raw_connection = mysql.connector.connect(**mysql_config)
        raw_cursor = raw_connection.cursor()

        for row in batch_df.collect():
            insert_raw = """
                INSERT INTO raw_emojis (user_id, emoji_type, timestamp)
                VALUES (%s, %s, %s)
            """
            raw_cursor.execute(insert_raw, (
                row['user_id'],
                row['emoji_type'],
                row['timestamp']
            ))

        raw_connection.commit()
        raw_cursor.close()
        raw_connection.close()
        print("\u2705 Raw data inserted into raw_emojis table")

    except mysql.connector.Error as raw_err:
        print("\u274c MySQL error while inserting raw data:", raw_err)

    end_time = time.time()
    duration = end_time - start_time

    # CPU & memory AFTER processing
    cpu_after = process.cpu_percent(interval=1)
    mem_after = process.memory_info().rss / 1024**2  # MB

    # Log resource usage
    print(f"\u23f1 Batch Execution Time: {duration:.2f} seconds")
    print(f"\u2699\ufe0f CPU Usage: {cpu_after:.2f}%")
    print(f"\U0001f9e0 Memory Usage: {mem_after:.2f} MB")
    print("-----------------------------------------------------\n")

# Stream trigger every 2 seconds
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="2 seconds") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
