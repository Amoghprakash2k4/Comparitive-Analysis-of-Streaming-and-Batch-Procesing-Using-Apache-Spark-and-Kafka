# stream_and_batch_metrics_comparison.py
import csv
import time
import psutil
from datetime import datetime

# Helper class to accumulate streaming batches
total_batches = []
stream_metrics_buffer = []

# Create CSV if not exists
csv_file = "performance_comparison.csv"
with open(csv_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["timestamp", "mode", "duration", "cpu_percent", "memory_used_MB"])


def log_metrics(mode, start_time, end_time, cpu_before, cpu_after, mem_before, mem_after):
    duration = end_time - start_time
    avg_cpu = (cpu_before + cpu_after) / 2
    mem_used = mem_after - mem_before

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, mode, f"{duration:.2f}", f"{avg_cpu:.2f}", f"{mem_used:.2f}"])

    return duration, avg_cpu, mem_used


# Function for batch mode analysis
def run_batch_analysis(spark, process):
    from pyspark.sql import SparkSession

    print("\n📊 Running batch analysis...")
    start_time = time.time()
    cpu_start = psutil.cpu_percent(interval=None)
    mem_start = process.memory_info().rss / 1024 ** 2

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/emostream") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "raw_emojis") \
        .option("user", "amogh") \
        .option("password", "yourpassword") \
        .load()

    df.createOrReplaceTempView("emojis")
    spark.sql("""
        SELECT emoji_type, COUNT(*) as count 
        FROM emojis 
        GROUP BY emoji_type 
        ORDER BY count DESC 
        LIMIT 10
    """).show(truncate=False)

    end_time = time.time()
    cpu_end = psutil.cpu_percent(interval=1)
    mem_end = process.memory_info().rss / 1024 ** 2

    duration, avg_cpu, mem_used = log_metrics("Batch", start_time, end_time, cpu_start, cpu_end, mem_start, mem_end)

    print(f"⏱ Batch Time: {duration:.2f}s | ⚙️ CPU: {avg_cpu:.2f}% | 🧠 Mem: {mem_used:.2f}MB")
    print("🔁 Waiting for next batch...\n")


# Function to process one 2-second streaming batch and log performance
def process_streaming_batch(process, start_time, end_time):
    cpu_start = psutil.cpu_percent(interval=None)
    mem_start = process.memory_info().rss / 1024 ** 2
    cpu_end = psutil.cpu_percent(interval=1)
    mem_end = process.memory_info().rss / 1024 ** 2

    stream_metrics_buffer.append((start_time, end_time, cpu_start, cpu_end, mem_start, mem_end))

    if len(stream_metrics_buffer) == 15:
        # Aggregate 15 batches (30 seconds)
        total_duration = sum(end - start for start, end, *_ in stream_metrics_buffer)
        total_cpu = sum((cpu_b + cpu_a) / 2 for *_, cpu_b, cpu_a, *_ in stream_metrics_buffer)
        total_mem = sum(mem_a - mem_b for *_, mem_b, mem_a in stream_metrics_buffer)

        avg_cpu = total_cpu / 15
        avg_duration = total_duration / 15
        avg_mem = total_mem / 15

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(csv_file, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, "Streaming-15x2s", f"{avg_duration:.2f}", f"{avg_cpu:.2f}", f"{avg_mem:.2f}"])

        print(f"\n🔁 15 Streaming Batches (30s) Aggregated")
        print(f"⏱ Avg Time: {avg_duration:.2f}s | ⚙️ Avg CPU: {avg_cpu:.2f}% | 🧠 Avg Mem: {avg_mem:.2f}MB\n")

        stream_metrics_buffer.clear()
