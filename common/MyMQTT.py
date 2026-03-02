import os, paho.mqtt.client as mqtt

class MQTTClient:
    def __init__(self, cid, host=None, port=None):
        host = host or os.getenv("MQTT_HOST","test.mosquitto.org")
        port = int(port or os.getenv("MQTT_PORT","1883"))
        self.c = mqtt.Client(client_id=cid, clean_session=True)
        self.c.connect(host, port, 60)
        self.c.loop_start()

    def sub(self, topic, on_message):
        def _on_msg(client, ud, msg):
            on_message(msg.topic, msg.payload.decode())
        self.c.subscribe(topic)
        self.c.on_message = _on_msg

    def pub(self, topic, payload, qos=0, retain=False):
        self.c.publish(topic, payload, qos=qos, retain=retain)
