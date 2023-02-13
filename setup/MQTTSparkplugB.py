from mqtt_spb_wrapper import (MqttSpbEntityApplication,
                              MqttSpbEntityDevice,
                              MqttSpbEntityEdgeNode,
                              MqttSpbEntityScada,
                              MqttSpbTopic)
from typing import Any, Dict

NoneType = type(None)


class MQTTSparkplugB:

    def __init__(self,
                 debug_info: bool = False,
                 filter_cmd_msg: bool = True,
                 spb_app_entity_name: Any = None,
                 spb_eon_device_name: Any = None,
                 spb_eon_name: Any = None,
                 spb_group_name: Any = None,
                 spb_scada_name: Any = None,
                 topic_str: str = "",
                 host: str = "127.0.0.1",
                 port: int = 1883,
                 user: str = "",
                 password: str = "",
                 use_tls: bool = False,
                 tls_ca_path: str = "",
                 tls_cert_path: str = "",
                 tls_key_path: str = "",
                 timeout: int = 5):
        self.debug_info = debug_info
        self.filter_cmd_msg = filter_cmd_msg
        self.spb_app_entity_name = spb_app_entity_name
        self.spb_eon_device_name = spb_eon_device_name
        self.spb_eon_name = spb_eon_name
        self.spb_group_name = spb_group_name
        self.spb_scada_name = spb_scada_name
        self.topic_str = topic_str
        self.entity = None

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.use_tls = use_tls
        self.tls_ca_path = tls_ca_path
        self.tls_cert_path = tls_cert_path
        self.tls_key_path = tls_key_path
        self.timeout = timeout

    def get_application(self) -> MqttSpbEntityApplication:
        if isinstance(self.entity, NoneType):
            self.entity = MqttSpbEntityApplication(spb_group_name=self.spb_group_name,
                                                   spb_app_entity_name=self.spb_app_entity_name,
                                                   debug_info=self.debug_info)
        return self.entity

    def get_device(self) -> MqttSpbEntityDevice:
        if isinstance(self.entity, NoneType):
            self.entity = MqttSpbEntityDevice(spb_group_name=self.spb_group_name,
                                              spb_eon_device_name=self.spb_eon_device_name,
                                              spb_eon_name=self.spb_eon_name,
                                              debug_info=self.debug_info,
                                              filter_cmd_msg=self.filter_cmd_msg)
        return self.entity

    def get_edge_node(self) -> MqttSpbEntityEdgeNode:
        if isinstance(self.entity, NoneType):
            self.entity = MqttSpbEntityEdgeNode(spb_group_name=self.spb_group_name,
                                                spb_eon_name=self.spb_eon_name,
                                                debug_info=self.debug_info)
        return self.entity

    def get_scada(self) -> MqttSpbEntityScada:
        if isinstance(self.entity, NoneType):
            self.entity = MqttSpbEntityScada(spb_group_name=self.spb_group_name,
                                             spb_scada_name=self.spb_scada_name,
                                             debug_info=self.debug_info)
        return self.entity

    def get_topic(self) -> MqttSpbTopic:
        return MqttSpbTopic(topic_str=self.topic_str)

    def get_attributes(self) -> list:
        return self.entity.attribures.get_dictionary()

    def set_attributes(self, attributes: Dict[str, str], timestamp: Any = None) -> None:
        for key, value in attributes.items():
            self.entity.attribures.set_value(name=key,
                                             value=value,
                                             timestamp=timestamp)

    def set_data_value(self, name: str, value: str, timestamp: Any = None) -> None:
        self.entity.data.set_value(name=name,
                                   value=value,
                                   timestamp=timestamp)

    def set_commands(self, commands: Dict[str, str], timestamp: Any = None) -> None:
        for key, value in commands.items():
            self.entity.commands.set_value(name=key,
                                           value=value,
                                           timestamp=timestamp)

    def connect(self) -> bool:
        return self.entity.connect(host=self.host,
                                   port=self.port,
                                   user=self.user,
                                   password=self.password,
                                   use_tls=self.use_tls,
                                   tls_ca_path=self.tls_ca_path,
                                   tls_cert_path=self.tls_cert_path,
                                   tls_key_path=self.tls_key_path,
                                   timeout=self.timeout)

    def publish_data(self) -> bool:
        return self.entity.publish_data()

    def disconnect(self) -> None:
        self.entity.disconnet()

    def print_message(self) -> None:
        def on_message(topic, payload):
            print(f"{topic}: {payload}")

        self.entity.on_message = on_message

    def print_command(self) -> None:
        def on_command(payload):
            print(f"payload: {payload}")

        self.entity.pn_command = on_command
