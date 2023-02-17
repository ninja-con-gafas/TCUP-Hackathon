#!/usr/bin/env bash

python3 "simulator/MQTTSparkplugBDataGenerator.py" "simulator/mqtt_sparkplug_b_data_generator_configuration.json" &
python3 "processor/DataStreamer.py" &
spark-submit --packages org.apache.bahir:spark-streaming-mqtt_2.11:2.4.0 "laboratory/experiment/read_mqtt_stream.py" &
wait
