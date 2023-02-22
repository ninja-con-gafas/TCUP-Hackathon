from json import load
from random import choice, uniform
from sys import argv, exit
from time import sleep
from typing import Dict, List
from wrapper.MQTTSparkplugB import MQTTSparkplugB


class MQTTSparkplugBDataGenerator:

    def __init__(self, configuration_file_path):
        try:
            with open(file=configuration_file_path,
                      mode='r') as configuration_file:
                configurations: Dict = load(configuration_file)
                mqtt_configuration: Dict = configurations.get("mqtt")
                miscellaneous_configuration: Dict = configurations.get("miscellaneous")
                mqtt_spb_entity_devices: Dict = configurations.get("mqtt_spb_entity_devices")

                self.host: str = mqtt_configuration.get("host")
                self.port: int = mqtt_configuration.get("port")

                self.interval_ms: float = miscellaneous_configuration.get("interval_ms")
                self.verbose: bool = miscellaneous_configuration.get("verbose")

                mqtt_spb_entity_devices_configuration: Dict = mqtt_spb_entity_devices.get("configuration")
                self.debug_info: bool = mqtt_spb_entity_devices_configuration.get("debug_info")
                self.filter_cmd_msg: bool = mqtt_spb_entity_devices_configuration.get("filter_cmd_msg")
                self.spb_eon_name: str = mqtt_spb_entity_devices_configuration.get("spb_eon_name")
                self.spb_group_name: str = mqtt_spb_entity_devices_configuration.get("spb_group_name")

                self.devices: Dict = mqtt_spb_entity_devices.get("devices")

            self.mqtt_sparkplug_b_entities: List[MQTTSparkplugB] = []

        except IOError as error:
            print(f"Error opening the configuration file: {error}")

    def create_mqtt_sparkplug_b_entities(self) -> None:
        for device in self.devices.keys():
            device_configuration: Dict = self.devices.get(device)
            attributes: Dict = device_configuration.get("attributes")
            spb_eon_device_name: str = attributes.get("device_id")

            mqtt_sparkplug_b_entity = MQTTSparkplugB(debug_info=self.debug_info,
                                                     filter_cmd_msg=self.filter_cmd_msg,
                                                     spb_eon_device_name=spb_eon_device_name,
                                                     spb_eon_name=self.spb_eon_name,
                                                     spb_group_name=self.spb_group_name,
                                                     host=self.host,
                                                     port=self.port)
            mqtt_sparkplug_b_entity.get_device()

            mqtt_sparkplug_b_entity.set_attributes(attributes=device_configuration)
            self.mqtt_sparkplug_b_entities.append(mqtt_sparkplug_b_entity)

    def connect_mqtt_broker(self) -> None:
        for entity in self.mqtt_sparkplug_b_entities:
            entity.connect_mqtt_broker()

    def generate_data(self) -> None:
        self.create_mqtt_sparkplug_b_entities()
        self.connect_mqtt_broker()

        interval_secs = self.interval_ms / 1000.0

        while True:
            entity: MQTTSparkplugB = choice(self.mqtt_sparkplug_b_entities)
            attributes: Dict = entity.get_attributes()[0].get("value")
            device_id: str = attributes.get("device_id")
            device_configuration: Dict = self.devices.get(device_id)

            lower_bound, upper_bound = device_configuration.get("range")
            value = round(uniform(lower_bound, upper_bound), 3)
            entity.set_data_value(name=device_id,
                                  value=str(value))

            if self.verbose:
                print(f"{device_id}: {value}")

            entity.publish_data()
            sleep(interval_secs)


if __name__ == "__main__":
    if len(argv) == 2:
        MQTTSparkplugBDataGenerator(argv[1]).generate_data()
    else:
        exit("Please provide a configuration file as command line argument.")
