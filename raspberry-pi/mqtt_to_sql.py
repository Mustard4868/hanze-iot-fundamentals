import os
import json
import pymysql
import paho.mqtt.client as mqtt
import time
from dotenv import load_dotenv

""" ENVIRONMENT """
load_dotenv()

DB_HOST         = "localhost"
DB_USER         = "user1"
DB_PASSWORD     = os.getenv("DB_PASSWORD")
DB_NAME         = "bme280"
DB_TABLE        = "sensors"

MQTT_BROKER     = "localhost"
MQTT_PORT       = 1883
MQTT_TOPIC      = "esp32/bme280"
MQTT_USER       = "user1"
MQTT_PASSWORD   = os.getenv("MQTT_PASSWORD")

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    message = msg.payload.decode("utf-8")
    print(f"Received message: {message}")
    data = json.loads(message)

    temperature = data.get("Temperature")
    pressure = data.get("Pressure")
    humidity = data.get("Humidity")
    timestamp = data.get("Timestamp")

    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, connect_timeout=60)
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {DB_TABLE} (temperature, pressure, humidity, timestamp) VALUES ({temperature}, {pressure}, {humidity}, '{timestamp}')")
        connection.commit()
        connection.close()
    except Exception as e:
        print(e)

try:
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
except Exception as e:
    print(e)

client.loop_forever()