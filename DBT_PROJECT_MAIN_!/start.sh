#!/bin/bash

echo "Starting Kafka and Zookeeper..."
sudo systemctl start kafka
sudo systemctl start zookeeper

sleep 10
echo "Starting subscribers..."
gnome-terminal --title="Subscriber 1 Cluster 1" -- bash -c "python3 subscriber1_cluster1.py; exec bash"
#gnome-terminal --title="Subscriber 1 Cluster 2" -- bash -c "python3 subscriber1_cluster2.py; exec bash"
#gnome-terminal --title="Subscriber 1 Cluster 3" -- bash -c "python3 subscriber1_cluster3.py; exec bash"
gnome-terminal --title="Subscriber 2 Cluster 1" -- bash -c "python3 subscriber2_cluster1.py; exec bash"
#gnome-terminal --title="Subscriber 2 Cluster 2" -- bash -c "python3 subscriber2_cluster2.py; exec bash"
#gnome-terminal --title="Subscriber 2 Cluster 3" -- bash -c "python3 subscriber2_cluster3.py; exec bash"
gnome-terminal --title="Subscriber 3 Cluster 1" -- bash -c "python3 subscriber3_cluster1.py; exec bash"
#gnome-terminal --title="Subscriber 3 Cluster 2" -- bash -c "python3 subscriber3_cluster2.py; exec bash"
#gnome-terminal --title="Subscriber 3 Cluster 3" -- bash -c "python3 subscriber3_cluster3.py; exec bash"

echo "Starting publishers..."
gnome-terminal --title="Publisher 1" -- bash -c "python3 publisher_1.py; exec bash"
#gnome-terminal --title="Publisher 2" -- bash -c "python3 publisher_2.py; exec bash"
#gnome-terminal --title="Publisher 3" -- bash -c "python3 publisher_3.py; exec bash"

# Run main publisher
echo "Starting main publisher..."
gnome-terminal --title="Main Publisher" -- bash -c "python3 mainpublisher2.py; exec bash"

# Run Spark job
echo "Starting Spark job..."
gnome-terminal --title="Spark Job" -- bash -c "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark2.py; exec bash"

echo "Starting server..."
gnome-terminal --title="Server" -- bash -c "./server_new.js; exec bash"

echo "Starting clients..."
gnome-terminal --title="Client 1" -- bash -c "./Client1.js; exec bash"
#gnome-terminal --title="Client 2" -- bash -c "./Client2.js; exec bash"
#gnome-terminal --title="Client 3" -- bash -c "./Client3.js; exec bash"
#gnome-terminal --title="Client 4" -- bash -c "./Client4.js; exec bash"
#gnome-terminal --title="Client 5" -- bash -c "./Client5.js; exec bash"
#gnome-terminal --title="Client 6" -- bash -c "./Client6.js; exec bash"
#gnome-terminal --title="Client 7" -- bash -c "./Client7.js; exec bash"
#gnome-terminal --title="Client 8" -- bash -c "./Client8.js; exec bash"
#gnome-terminal --title="Client 9" -- bash -c "./Client9.js; exec bash"
#gnome-terminal --title="Client 10" -- bash -c "./Client10.js; exec bash"
#gnome-terminal --title="Client 11" -- bash -c "./Client11.js; exec bash"
#gnome-terminal --title="Client 12" -- bash -c "./Client12.js; exec bash"
#gnome-terminal --title="Client 13" -- bash -c "./Client13.js; exec bash"
#gnome-terminal --title="Client 14" -- bash -c "./Client14.js; exec bash"
#gnome-terminal --title="Client 15" -- bash -c "./Client15.js; exec bash"
#gnome-terminal --title="Client 16" -- bash -c "./Client16.js; exec bash"
#gnome-terminal --title="Client 17" -- bash -c "./Client17.js; exec bash"
#gnome-terminal --title="Client 18" -- bash -c "./Client18.js; exec bash"

echo "Starting sender clients..."
gnome-terminal --title="Sender Client 1" -- bash -c "./SenderClient1.js; exec bash"
#gnome-terminal --title="Sender Client 2" -- bash -c "./SenderClient2.js; exec bash"
#gnome-terminal --title="Sender Client 3" -- bash -c "./SenderClient3.js; exec bash"

echo "All processes have been started in separate terminals."


sleep 30  # wait 60 seconds for data to accumulate
echo "Running batch analysis..."
gnome-terminal --title="Batch Analysis" -- bash -c "python3 batch_analysis.py; exec bash"
