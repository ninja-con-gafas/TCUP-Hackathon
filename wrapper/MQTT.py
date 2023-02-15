from paho.mqtt.client import Client
from paho.mqtt.client import MQTTMessageInfo, MQTTMessage
from typing import Any


class MQTT:
    def __init__(self,
                 client_id: str = "",
                 clean_session: bool = None,
                 userdata: Any = None,
                 transport: str = "tcp",
                 reconnect_on_failure: bool = True,
                 host: str = "127.0.0.1",
                 port: int = 1883,
                 time_out: int = 60,
                 bind_address: str = "",
                 bind_port: int = 0,
                 properties: Any = None
                 ):
        self.client_id = client_id
        self.clean_session = clean_session
        self.userdata = userdata
        self.transport = transport
        self.reconnect_on_failure = reconnect_on_failure
        self.host = host
        self.port = port
        self.time_out = time_out
        self.bind_address = bind_address
        self.bind_port = bind_port
        self.properties = properties
        self.mqtt_client = Client(client_id=self.client_id,
                                  clean_session=self.clean_session,
                                  userdata=self.userdata,
                                  transport=self.transport,
                                  reconnect_on_failure=self.reconnect_on_failure)

    def get_mqtt_client(self) -> Client:
        return self.mqtt_client

    def connect_mqtt_broker(self) -> None:
        self.mqtt_client.connect(host=self.host,
                                 port=self.port,
                                 keepalive=self.time_out,
                                 bind_address=self.bind_address,
                                 bind_port=self.bind_port,
                                 properties=self.properties)

    def publish(self,
                mqtt_topic: str,
                payload: str,
                quality_of_service: int,
                retain: bool = False,
                properties: Any = None) -> MQTTMessageInfo:
        return self.mqtt_client.publish(topic=mqtt_topic,
                                        payload=payload,
                                        qos=quality_of_service,
                                        retain=retain,
                                        properties=properties)

    def print_message(self, mqtt_topic: str) -> None:
        def on_message(client_: Client, userdata: Any, message: MQTTMessage):
            print(f"Message: {message.payload.decode()}")

        self.mqtt_client.subscribe(mqtt_topic)
        self.mqtt_client.on_message = on_message
