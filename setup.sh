# Prerequisites:
#   Ubuntu 20.04 LTS
#   A non-root user with sudo rights

echo "Download package information from all configured sources"
sudo apt-get update

echo "Add deadsnakes to Personal Package Archive PPA"
sudo add-apt-repository -y ppa:deadsnakes/ppa

echo "Install git Python version 3.7, pip3 and venv module version 3.7"
sudo apt-get install -y git python3.7-dev python3-pip python3.7-venv

echo "Path of the Python 3.7 version"
which python3.7

echo "Install Java and mosquitto MQTT broker"
sudo apt-get install -y default-jdk default-jre mosquitto

echo "Ensure mosquitto is active and running"
sudo systemctl status mosquitto