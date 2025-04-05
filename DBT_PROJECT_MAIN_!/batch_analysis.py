import time
import psutil
from pyspark.sql import SparkSession
from performance_logger import process_batch_analysis

# ✅ Create Spark Session
spark = SparkSession.builder \
    .appName("BatchEmojiAnalysis") \
    .config("spark.driver.extraClassPath", "/home/pes2ug22cs062/Downloads/mysql_jdbc/usr/share/java/mysql-connector-java-9.2.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ Monitoring: current process
process = psutil.Process()

while True:
    print("📊 Running batch analysis...")

    # Start time & resource usage before
    start_time = time.time()
    cpu_start = psutil.cpu_percent(interval=None)
    mem_start = process.memory_info().rss / 1024**2  # MB

    # ✅ Load data from MySQL
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/emostream") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "raw_emojis") \
        .option("user", "amogh") \
        .option("password", "yourpassword") \
        .load()

    df.createOrReplaceTempView("emojis")

    # ✅ Perform batch analysis
    result = spark.sql("""
        SELECT emoji_type, COUNT(*) as count 
        FROM emojis 
        GROUP BY emoji_type 
        ORDER BY count DESC 
        LIMIT 10
    """)
    result.show(truncate=False)

    # ✅ Extract top emojis for logger
    top_emojis = [
        {"emoji": row["emoji_type"], "count": row["count"]}
        for row in result.collect()
    ]

    # End time & resource usage after
    end_time = time.time()
    cpu_end = psutil.cpu_percent(interval=1)
    mem_end = process.memory_info().rss / 1024**2  # MB

    # 📈 Metrics
    duration = end_time - start_time
    avg_cpu = (cpu_start + cpu_end) / 2
    mem_used = mem_end - mem_start

    # ✅ Print metrics
    print(f"⏱ Batch Execution Time: {duration:.2f} seconds")
    print(f"⚙️ CPU Usage: ~{avg_cpu:.2f}%")
    print(f"🧠 Memory Used This Run: {mem_used:.2f} MB")

    # ✅ Log performance
    process_batch_analysis(duration, avg_cpu, mem_used, top_emojis)

    print("🔁 Waiting 30 seconds for next batch...\n")
    time.sleep(30)
