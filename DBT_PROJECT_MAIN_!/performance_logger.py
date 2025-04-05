import csv
import os
from datetime import datetime

# Store the last 15 streaming batch metrics
streaming_metrics_buffer = []
latest_streaming_aggregated = None
latest_batch = None
latest_streaming_emojis = None
latest_batch_emojis = None

# CSV file path
csv_file = "performance_comparison.csv"

# Header for CSV
csv_header = ["Batch_Type", "Timestamp", "Execution_Time", "CPU_Usage", "Memory_Usage"]

# Initialize CSV if not exists
def init_csv():
    if not os.path.exists(csv_file):
        with open(csv_file, mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)

# Append a row to CSV
def write_row(batch_type, duration, cpu, mem, timestamp):
    with open(csv_file, mode='a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([batch_type, timestamp, f"{duration:.2f}", f"{cpu:.2f}", f"{mem:.2f}"])

# Compare top emojis from batch and streaming
def compare_emoji_results():
    global latest_streaming_emojis, latest_batch_emojis

    if latest_streaming_emojis is not None and latest_batch_emojis is not None:
        print("\n🔍 Comparing Top Emojis from Streaming and Batch...")

        if latest_streaming_emojis == latest_batch_emojis:
            print("✅ Emoji results match! ✔️")
        else:
            print("⚠️ Emoji results differ!")
            print(f"   Streaming: {latest_streaming_emojis}")
            print(f"   Batch    : {latest_batch_emojis}")

        latest_streaming_emojis = None
        latest_batch_emojis = None

# Check and log both Streaming_Aggregated and Batch if available
def log_if_both_ready():
    global latest_streaming_aggregated, latest_batch
    if latest_streaming_aggregated and latest_batch:
        stream_ts, stream_data = latest_streaming_aggregated
        batch_ts, batch_data = latest_batch

        print(f"\n📊 Synced Log - Streaming Aggregated and Batch at {stream_ts}:")
        print(f"   Streaming_Aggregated -> Time: {stream_data[0]:.2f}s, CPU: {stream_data[1]:.2f}%, Mem: {stream_data[2]:.2f} MB")
        print(f"   Batch               -> Time: {batch_data[0]:.2f}s, CPU: {batch_data[1]:.2f}%, Mem: {batch_data[2]:.2f} MB\n")

        latest_streaming_aggregated = None
        latest_batch = None

# Call this from Spark Streaming after each 2-sec batch
def process_streaming_batch(batch_id, duration, cpu, mem, top_emojis):
    init_csv()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    write_row("Streaming", duration, cpu, mem, timestamp)

    streaming_metrics_buffer.append((duration, cpu, mem))

    if len(streaming_metrics_buffer) == 15:
        # Aggregate 15 batches = 30 seconds
        avg_duration = sum(x[0] for x in streaming_metrics_buffer)
        avg_cpu = sum(x[1] for x in streaming_metrics_buffer) / 15
        avg_mem = sum(x[2] for x in streaming_metrics_buffer) / 15

        stream_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        global latest_streaming_aggregated, latest_streaming_emojis
        latest_streaming_aggregated = (stream_ts, (avg_duration, avg_cpu, avg_mem))
        latest_streaming_emojis = top_emojis

        write_row("Streaming_Aggregated", avg_duration, avg_cpu, avg_mem, stream_ts)
        print(f"✅ Logged Streaming_Aggregated at {stream_ts}")

        streaming_metrics_buffer.clear()
        log_if_both_ready()
        compare_emoji_results()

# Call this from batch analysis script
def process_batch_analysis(duration, cpu, mem, top_emojis):
    init_csv()
    batch_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    global latest_batch, latest_batch_emojis
    latest_batch = (batch_ts, (duration, cpu, mem))
    latest_batch_emojis = top_emojis

    write_row("Batch", duration, cpu, mem, batch_ts)
    print(f"✅ Logged Batch at {batch_ts}")

    log_if_both_ready()
    compare_emoji_results()

# Optional: Clear the CSV file manually (use with caution)
def clear_csv():
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(csv_header)
    print("🧹 Cleared CSV file and reset header.")
