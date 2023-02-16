from wrapper.MQTT import MQTT

mqtt = MQTT()
mqtt.connect_mqtt_broker()
mqtt_client = mqtt.get_mqtt_client()
mqtt.print_message(mqtt_topic="ice_plant")
mqtt_client.loop_forever()
