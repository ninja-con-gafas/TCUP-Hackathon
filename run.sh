#!/usr/bin/env bash

echo "Start the Apache Spark Streaming Application"
spark-submit --packages org.apache.bahir:spark-streaming-mqtt_2.11:2.4.0 "processor/DataProcessor.py" &
echo "Wait for 30 seconds to get the application running"
sleep 30

echo "Start the Sparkplug B MQTT IoT device simulator"
python3 "simulator/MQTTSparkplugBDataGenerator.py" "simulator/mqtt_sparkplug_b_data_generator_configuration.json" &

echo "Start the data streamer"
python3 "processor/DataStreamer.py" &
wait
