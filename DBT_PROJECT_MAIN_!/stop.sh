#!/bin/bash
pkill -f 'Client1.js'
pkill -f 'Client2.js'
pkill -f 'Client3.js'
pkill -f 'Client4.js'
pkill -f 'Client5.js'
pkill -f 'Client6.js'
pkill -f 'Client7.js'
pkill -f 'Client8.js'
pkill -f 'Client9.js'
pkill -f 'Client10.js'
pkill -f 'Client11.js'
pkill -f 'Client12.js'
pkill -f 'Client13.js'
pkill -f 'Client14.js'
pkill -f 'Client15.js'
pkill -f 'Client16.js'
pkill -f 'Client17.js'
pkill -f 'Client18.js'


echo "Stopping all subscribers..."
pkill -f subscriber1_cluster1.py
pkill -f subscriber1_cluster2.py
pkill -f subscriber1_cluster3.py
pkill -f subscriber2_cluster1.py
pkill -f subscriber2_cluster2.py
pkill -f subscriber2_cluster3.py
pkill -f subscriber3_cluster1.py
pkill -f subscriber3_cluster2.py
pkill -f subscriber3_cluster3.py

echo "Stopping all publishers..."
pkill -f publisher_1.py
pkill -f publisher_2.py
pkill -f publisher_3.py
pkill -f mainpublisher2.py

echo "Stopping server and clients..."
pkill -f server_new.js
pkill -f SenderClient1.js
pkill -f SenderClient2.js
pkill -f SenderClient3.js

echo "Stopping Spark job..."
pkill -f pyspark2.py
pkill -f batch_analysis.py

echo "All processes have been stopped."
