# prerequisites:
#   Ubuntu 20.04 LTS
#   a non-root user with sudo rights

# install mosquitto
sudo apt-get install -y mosquitto

# ensure package is loaded and active
sudo systemctl status mosquitto