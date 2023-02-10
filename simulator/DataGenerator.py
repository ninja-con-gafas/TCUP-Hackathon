import json
from sys import argv
from setup.MQTT import MQTT
import random
import time


class DataGenerator:

    def __init__(self, configuration_file_path) -> None:
        try:
            with open(file=configuration_file_path,
                      mode='r') as configuration_file:
                configurations = json.load(configuration_file)
                mqtt_configuration = configurations.get("mqtt")
                miscellaneous_configuration = configurations.get("miscellaneous")
                self.sensors = configurations.get("sensors", False)

                self.username = mqtt_configuration.get("username", False)
                self.password = mqtt_configuration.get("password", None)
                self.host = mqtt_configuration.get("host", "127.0.0.1")
                self.port = mqtt_configuration.get("port", 1883)
                self.topic = mqtt_configuration.get("topic", False)

                self.interval_ms = miscellaneous_configuration.get("interval_ms", 1000)
                self.verbose = miscellaneous_configuration.get("verbose", False)

                if not self.topic:
                    print("No topic specified in configurations.")
                    return

                if not self.sensors:
                    print("No sensors specified in configurations.")
                    return

        except IOError as error:
            print(f"Error opening the configuration file: {error}")

    def generate(self) -> None:
        mqtt = MQTT(host=self.host,
                    port=self.port)
        client = mqtt.get_mqtt_client()

        if self.username:
            client.username_pw_set(self.username, self.password)

        mqtt.connect_mqtt_broker()

        keys = list(self.sensors.keys())
        interval_secs = self.interval_ms / 1000.0

        while True:
            sensor_id = random.choice(keys)
            sensor = self.sensors[sensor_id]
            lower_bound, upper_bound = sensor.get("range", [])
            value = round(random.uniform(lower_bound, upper_bound), 3)

            data = {
                "id": sensor_id,
                "value": value
            }

            for key in ["sensor_type", "lat", "lng", "unit", "description"]:
                value = sensor.get(key)

                if value is not None:
                    data[key] = value

            payload = json.dumps(data)

            if self.verbose:
                print(f"{self.topic}:{payload}")

            mqtt.publish(mqtt_topic=self.topic,
                         payload=payload,
                         quality_of_service=0)
            time.sleep(interval_secs)


if __name__ == "__main__":
    if len(argv) == 2:
        DataGenerator(argv[1]).generate()
    else:
        print("Please provide a configuration file as command line argument.")
