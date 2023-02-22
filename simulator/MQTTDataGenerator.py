from json import load, dumps
from random import choice, uniform
from sys import argv, exit
from time import sleep
from typing import Dict
from wrapper.MQTT import MQTT


class MQTTDataGenerator:

    def __init__(self, configuration_file_path):
        try:
            with open(file=configuration_file_path,
                      mode='r') as configuration_file:
                configurations: Dict = load(configuration_file)
                mqtt_configuration: Dict = configurations.get("mqtt")
                miscellaneous_configuration: Dict = configurations.get("miscellaneous")

                self.host: str = mqtt_configuration.get("host")
                self.port: int = mqtt_configuration.get("port")
                self.topic: str = mqtt_configuration.get("topic")

                self.interval_ms: float = miscellaneous_configuration.get("interval_ms")
                self.verbose: bool = miscellaneous_configuration.get("verbose")

                self.sensors: Dict = configurations.get("sensors")

        except IOError as error:
            print(f"Error opening the configuration file: {error}")

    def generate_data(self) -> None:
        mqtt = MQTT(host=self.host,
                    port=self.port)
        mqtt.connect_mqtt_broker()

        attributes = list(self.sensors.keys())
        interval_secs = self.interval_ms / 1000.0

        while True:
            sensor_id: str = choice(attributes)
            sensor: Dict = self.sensors[sensor_id]
            lower_bound, upper_bound = sensor.get("range")
            value = round(uniform(lower_bound, upper_bound), 3)

            data = {
                "id": sensor_id,
                "value": value
            }

            for attribute in ["sensor_type", "lat", "lng", "unit", "description"]:
                value = sensor.get(attribute)

                if value is not None:
                    data[attribute] = value

            payload = dumps(data)

            if self.verbose:
                print(f"{self.topic}:{payload}")

            mqtt.publish(mqtt_topic=self.topic,
                         payload=payload,
                         quality_of_service=0)
            sleep(interval_secs)


if __name__ == "__main__":
    if len(argv) == 2:
        MQTTDataGenerator(argv[1]).generate_data()
    else:
        exit("Please provide a configuration file as command line argument.")
