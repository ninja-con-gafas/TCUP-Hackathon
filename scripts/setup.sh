#!/usr/bin/env bash

# Prerequisites:
#   Ubuntu 20.04 LTS
#   A non-root user with sudo rights
URL_MYSQL_CONNECTOR_JAVA="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar"
DOWNLOAD_PATH_MYSQL_CONNECTOR_JAVA="$HOME/Downloads/mysql-connector-j-8.0.32.jar"

echo ">>> Download package information from all configured sources"
sudo apt-get update

echo ">>> Add deadsnakes to Personal Package Archive"
sudo add-apt-repository -y ppa:deadsnakes/ppa

echo ">>> Install git Python version 3.7, pip3 and venv module version 3.7"
sudo apt-get install -y git python3.7-dev python3-pip python3.7-venv

echo ">>> Path of the Python 3.7 version"
which python3.7

echo ">>> Install Java, MySQL server and mosquitto MQTT broker"
sudo apt-get install -y default-jdk=2:1.11-72 default-jre=2:1.11-72 mysql-server=8.0.32-0ubuntu0.20.04.2 mosquitto=1.6.9-1

echo ">>> Download mysql-connector-j-8.0.32.jar"
wget -O "$DOWNLOAD_PATH_MYSQL_CONNECTOR_JAVA" "$URL_MYSQL_CONNECTOR_JAVA"

echo ">>> Once the virtual environment is created, place the jar file here:"
echo "    venv/lib/python3.7/site-packages/pyspark/jars"

echo ">>> Setup complete"
exit 0