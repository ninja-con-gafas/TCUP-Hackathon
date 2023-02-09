from paho.mqtt.client import Client
from paho.mqtt.client import MQTTMessageInfo, MQTTMessage
from typing import Any
from time import sleep


class MQTTClient:

    @staticmethod
    def get_mqtt_client(client_id: str = "",
                        clean_session: bool = None,
                        userdata: Any = None,
                        transport: str = "tcp",
                        reconnect_on_failure: bool = True,
                        broker_address: str = "127.0.0.1",
                        port: int = 1883,
                        time_out: int = 60,
                        bind_address: str = "",
                        bind_port: int = 0,
                        properties: Any = None) -> Client:
        def on_connect(client_: Client, userdata_: Any, flags: dict, rc: int):
            print(f"client: {client_}, userdata: {userdata_}, flags: {flags}, rc: {rc}")

        mqtt_client = Client(client_id=client_id,
                             clean_session=clean_session,
                             userdata=userdata,
                             transport=transport,
                             reconnect_on_failure=reconnect_on_failure)
        mqtt_client.on_connect = on_connect
        mqtt_client.connect(host=broker_address,
                            port=port,
                            keepalive=time_out,
                            bind_address=bind_address,
                            bind_port=bind_port,
                            properties=properties)
        return mqtt_client


class Publisher:

    def __init__(self, mqtt_client):
        self.client = mqtt_client

    def publish(self,
                mqtt_topic: str,
                payload: str,
                quality_of_service: int,
                retain: bool = False,
                properties: Any = None) -> MQTTMessageInfo:
        return self.client.publish(topic=mqtt_topic,
                                   payload=payload,
                                   qos=quality_of_service,
                                   retain=retain,
                                   properties=properties)


class Subscriber:

    def __init__(self, mqtt_client):
        self.client = mqtt_client

    def subscribe(self, mqtt_topic: str):
        def on_message(client_: Client, userdata: Any, message: MQTTMessage):
            print(f"client {client_}, userdata: {userdata}, message: {message.payload.decode()}")

        self.client.subscribe(mqtt_topic)
        self.client.on_message = on_message


if __name__ == "__main__":
    client = MQTTClient.get_mqtt_client()
    client.loop_start()

    message_number = 0
    topic = "test_topic"
    while True:
        message_number += 1
        message_info = Publisher(mqtt_client=client).publish(mqtt_topic=topic,
                                                             payload=f"This is message number {message_number}",
                                                             quality_of_service=0)
        print("Is message published? " + ("No" if message_info[0] else "Yes"))
        sleep(1)
        Subscriber(mqtt_client=client).subscribe(mqtt_topic=topic)
