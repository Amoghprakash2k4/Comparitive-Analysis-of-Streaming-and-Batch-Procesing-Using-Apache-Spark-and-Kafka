import time
import psutil
from pyspark.sql import SparkSession

# ✅ Create Spark Session
spark = SparkSession.builder \
    .appName("BatchEmojiAnalysis") \
    .config("spark.driver.extraClassPath", "/home/pes2ug22cs062/Downloads/mysql_jdbc/usr/share/java/mysql-connector-java-9.2.0.jar") \
    .getOrCreate()

# Optional: reduce logging noise
spark.sparkContext.setLogLevel("WARN")

# ✅ Monitoring: current process
process = psutil.Process()

while True:
    print("📊 Running batch analysis...")

    # Start time & resource usage before
    start_time = time.time()
    cpu_start = psutil.cpu_percent(interval=None)
    mem_start = process.memory_info().rss / 1024**2  # in MB

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

    # End time & resource usage after
    end_time = time.time()
    cpu_end = psutil.cpu_percent(interval=1)
    mem_end = process.memory_info().rss / 1024**2

    # 🔍 Metrics
    duration = end_time - start_time
    avg_cpu = (cpu_start + cpu_end) / 2
    mem_used = mem_end - mem_start

    # ✅ Print results
    print(f"⏱ Batch Execution Time: {duration:.2f} seconds")
    print(f"⚙️ CPU Usage: ~{avg_cpu:.2f}%")
    print(f"🧠 Memory Used This Run: {mem_used:.2f} MB")
    print("🔁 Waiting 60 seconds for next batch...\n")

    # ✅ Wait before next batch
    time.sleep(30)
