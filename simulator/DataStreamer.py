from json import dumps, load
from sys import argv, exit
from typing import Dict
from wrapper.MQTT import MQTT
from wrapper.MQTTSparkplugB import MQTTSparkplugB, MqttSpbEntityApplication


class DataStreamer:

    def __init__(self, configuration_file_path):
        try:
            with open(file=configuration_file_path,
                      mode='r') as configuration_file:
                configurations: Dict = load(configuration_file)
                mqtt_configuration: Dict = configurations.get("mqtt")
                mqtt_spb_entity_devices: Dict = configurations.get("mqtt_spb_entity_devices")
                mqtt_spb_entity_devices_configuration: Dict = mqtt_spb_entity_devices.get("configuration")

                self.spb_app_entity_name = mqtt_spb_entity_devices_configuration.get("spb_eon_name")
                self.spb_group_name: str = mqtt_spb_entity_devices_configuration.get("spb_group_name")
                self.host: str = mqtt_configuration.get("host")
                self.port: int = mqtt_configuration.get("port")
                self.debug_info: bool = mqtt_spb_entity_devices_configuration.get("debug_info")

            self.mqtt_sparkplug_b_entity: MQTTSparkplugB = self.get_sparkplug_b_entity()
            self.mqtt_sparkplug_b_application: MqttSpbEntityApplication = self.get_mqtt_sparkplug_b_application()
            self.mqtt: MQTT = MQTT()

        except IOError as error:
            print(f"Error opening the configuration file: {error}")

    def get_sparkplug_b_entity(self) -> MQTTSparkplugB:
        return MQTTSparkplugB(debug_info=self.debug_info,
                              spb_app_entity_name=self.spb_app_entity_name,
                              spb_group_name=self.spb_group_name,
                              host=self.host,
                              port=self.port)

    def get_mqtt_sparkplug_b_application(self) -> MqttSpbEntityApplication:
        return self.mqtt_sparkplug_b_entity.get_application()

    def connect_mqtt_broker(self) -> None:
        self.mqtt_sparkplug_b_entity.connect_mqtt_broker()
        self.mqtt.connect_mqtt_broker()

    def publish_decoded_payload(self) -> None:
        def on_message(topic, payload):
            self.mqtt.publish(mqtt_topic=self.spb_group_name,
                              payload=dumps({"data": [str(topic), str(payload)]}),
                              quality_of_service=0)

        self.mqtt_sparkplug_b_application.on_message = on_message

    def loop(self):
        while True:
            self.publish_decoded_payload()


if __name__ == "__main__":
    if len(argv) == 2:
        streamer = DataStreamer(argv[1])
        streamer.connect_mqtt_broker()
        streamer.loop()
    else:
        exit("Please provide a configuration file as command line argument.")
