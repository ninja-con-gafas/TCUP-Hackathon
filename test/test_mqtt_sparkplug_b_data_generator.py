from wrapper.MQTTSparkplugB import MQTTSparkplugB

debug_info = True
spb_group_name = "ice_plant"
spb_app_entity_name = "sensors"

mqtt_sparkplug_b_entity = MQTTSparkplugB(debug_info=debug_info,
                                         spb_app_entity_name=spb_app_entity_name,
                                         spb_group_name=spb_group_name)

mqtt_sparkplug_b_entity.get_application()
mqtt_sparkplug_b_entity.connect_mqtt_broker()

while True:
    mqtt_sparkplug_b_entity.print_message()
