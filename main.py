import paho.mqtt.client as mqtt
import threading
import time
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

LOCAL_BROKER = os.getenv("LOCAL_BROKER", "mosquitto")
LOCAL_PORT = int(os.getenv("LOCAL_PORT", 1883))

REMOTE_BROKER = os.getenv("REMOTE_BROKER")
REMOTE_PORT = int(os.getenv("REMOTE_PORT", 443))
REMOTE_USER = os.getenv("REMOTE_USER")
REMOTE_PASS = os.getenv("REMOTE_PASS")
REMOTE_PATH = os.getenv("REMOTE_PATH", "/ws")

TOPIC = os.getenv("TOPIC", "#")

# Prevent message loops
BRIDGE_TAG = "bridge-origin"

def tag_payload(payload):
    return payload + f" [{BRIDGE_TAG}]".encode()

def is_from_bridge(payload):
    return payload.endswith(f"[{BRIDGE_TAG}]".encode())

def strip_tag(payload):
    return payload.replace(f" [{BRIDGE_TAG}]".encode(), b"")

def on_local_message(client, userdata, msg):
    if is_from_bridge(msg.payload):
        return
    logging.info(f"LOCAL → REMOTE {msg.topic}")
    remote_client.publish(msg.topic, tag_payload(msg.payload), qos=1)

def on_remote_message(client, userdata, msg):
    if is_from_bridge(msg.payload):
        return
    logging.info(f"REMOTE → LOCAL {msg.topic}")
    local_client.publish(msg.topic, tag_payload(msg.payload), qos=1)

def connect_local():
    while True:
        try:
            local_client.connect(LOCAL_BROKER, LOCAL_PORT, 60)
            return
        except Exception as e:
            logging.error(f"Local connect failed: {e}")
            time.sleep(5)

def connect_remote():
    while True:
        try:
            remote_client.connect(REMOTE_BROKER, REMOTE_PORT, 60)
            return
        except Exception as e:
            logging.error(f"Remote connect failed: {e}")
            time.sleep(5)

# Local MQTT (Mosquitto)
local_client = mqtt.Client(client_id="bridge-local", protocol=mqtt.MQTTv5)
local_client.on_message = on_local_message

# Remote MQTT over WSS (RabbitMQ)
remote_client = mqtt.Client(client_id="bridge-remote", transport="websockets", protocol=mqtt.MQTTv5)
remote_client.ws_set_options(path=REMOTE_PATH)
remote_client.username_pw_set(REMOTE_USER, REMOTE_PASS)
remote_client.tls_set()
remote_client.on_message = on_remote_message

logging.info("Connecting local...")
connect_local()
local_client.subscribe(TOPIC)

logging.info("Connecting remote...")
connect_remote()
remote_client.subscribe(TOPIC)

# Start loops
threading.Thread(target=local_client.loop_forever, daemon=True).start()
threading.Thread(target=remote_client.loop_forever, daemon=True).start()

# Keep alive
while True:
    time.sleep(1)