from json import dumps
from wrapper.MQTT import MQTT
from wrapper.MQTTSparkplugB import MQTTSparkplugB, MqttSpbEntityApplication


class DataStreamer:

    def __init__(self,
                 spb_app_entity_name: str,
                 spb_group_name: str,
                 host: str = "127.0.0.1",
                 port: int = 1883,
                 user: str = "",
                 password: str = "",
                 debug_info: bool = False):
        self.spb_app_entity_name = spb_app_entity_name
        self.spb_group_name = spb_group_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.debug_info = debug_info

        self.mqtt_sparkplug_b_entity: MQTTSparkplugB = self.get_sparkplug_b_entity()
        self.mqtt_sparkplug_b_application: MqttSpbEntityApplication = self.get_mqtt_sparkplug_b_application()
        self.mqtt: MQTT = MQTT()

    def get_sparkplug_b_entity(self) -> MQTTSparkplugB:
        return MQTTSparkplugB(debug_info=self.debug_info,
                              spb_app_entity_name=self.spb_app_entity_name,
                              spb_group_name=self.spb_group_name,
                              host=self.host,
                              port=self.port,
                              user=self.user,
                              password=self.password)

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
    streamer = DataStreamer(spb_app_entity_name="sensors",
                            spb_group_name="ice_plant",
                            host="127.0.0.1",
                            port=1883)
    streamer.connect_mqtt_broker()
    streamer.loop()
