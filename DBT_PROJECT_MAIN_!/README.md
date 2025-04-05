# Emoji Stream & Batch Analytics System

A real-time and batch emoji analytics system using Apache Kafka, Apache Spark (Streaming & SQL), and MySQL.

---

## 🧱 Architecture Overview

![Architecture Diagram](architecture.png)

**Modules**:
- **Kafka Producer**: Feeds emoji data to `emoji_topic`
- **Spark Streaming**: Processes live Kafka data in 2-sec microbatches
- **Spark Batch**: Periodically loads emoji data from MySQL for offline analysis
- **MySQL**: Stores raw and processed emoji data
- **Performance Logger**: Logs CPU, memory, and execution time into CSV for comparison

---

## ⚙️ Requirements

- Python 3.x
- Apache Kafka & Zookeeper
- Apache Spark
- MySQL
- PySpark
- psutil
- kafka-python
- seaborn (for visualization)

---

## 🚀 Running the Project

### 1. Kafka Setup

```bash
# Start spark
/opt/spark/sbin/start-all.sh

# Start Kafka 
sudo systemctl start kafka

# Run
./start.sh
