import json
from sys import argv
from wrapper.MQTTSparkplugB import MQTTSparkplugB
import random
import time


class MQTTSparkplugBDataGenerator:

    def __init__(self, configuration_file_path):
        try:
            with open(file=configuration_file_path,
                      mode='r') as configuration_file:
                configurations = json.load(configuration_file)
                mqtt_configuration = configurations.get("mqtt")
                miscellaneous_configuration = configurations.get("miscellaneous")
                mqtt_spb_entity_devices = configurations.get("mqtt_spb_entity_devices")

                self.host = mqtt_configuration.get("host")
                self.port = mqtt_configuration.get("port")

                self.interval_ms = miscellaneous_configuration.get("interval_ms")
                self.verbose = miscellaneous_configuration.get("verbose")

                mqtt_spb_entity_devices_configuration = mqtt_spb_entity_devices.get("configuration")
                self.debug_info = mqtt_spb_entity_devices_configuration.get("debug_info")
                self.filter_cmd_msg = mqtt_spb_entity_devices_configuration.get("filter_cmd_msg")
                self.spb_eon_name = mqtt_spb_entity_devices_configuration.get("spb_eon_name")
                self.spb_group_name = mqtt_spb_entity_devices_configuration.get("spb_group_name")

                self.devices = mqtt_spb_entity_devices.get("devices")

            self.mqtt_sparkplug_b_entities = []

        except IOError as error:
            print(f"Error opening the configuration file: {error}")

    def create_mqtt_sparkplug_b_entities(self) -> None:
        for device in self.devices.keys():
            device_configuration = self.devices.get(device)
            attributes = device_configuration.get("attributes")
            spb_eon_device_name = attributes.get("device_id")

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
            entity.connect()

    def generate_data(self) -> None:
        self.create_mqtt_sparkplug_b_entities()
        self.connect_mqtt_broker()

        interval_secs = self.interval_ms / 1000.0

        while True:
            entity = random.choice(self.mqtt_sparkplug_b_entities)
            attributes = entity.get_attributes()[0].get("value")
            device_id = attributes.get("device_id")
            device_configuration = self.devices.get(device_id)

            lower_bound, upper_bound = device_configuration.get("range")
            value = round(random.uniform(lower_bound, upper_bound), 3)
            entity.set_data_value(name="value",
                                  value=value)

            if self.verbose:
                print(f"{device_id}: {value}")

            entity.publish_data()
            time.sleep(interval_secs)


if __name__ == "__main__":
    if len(argv) == 2:
        MQTTSparkplugBDataGenerator(argv[1]).generate_data()
    else:
        print("Please provide a configuration file as command line argument.")
