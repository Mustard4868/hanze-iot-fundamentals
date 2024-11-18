# https://github.com/mustard4868/hanze-iot-fundamentals
# Monday, November 18, 2024

import pymysql
import os
import json
import time
import paho.mqtt.client as mqtt
from azure.iot.device import IoTHubDeviceClient, Message
from threading import Thread
from dotenv import load_dotenv

load_dotenv()

SERVER_IP       = "192.168.2.149"
DB_HOST         = SERVER_IP
DB_USER         = "user1"
DB_PASSWORD     = os.getenv("DB_PASSWORD")
DB_NAME         = "bme280"
DB_TABLE        = "sensors"
MQTT_BROKER     = SERVER_IP
MQTT_PORT       = 1883
MQTT_TOPIC      = "esp32/bme280"
MQTT_USER       = "user1"
MQTT_PASSWORD   = os.getenv("MQTT_PASSWORD")
IOTHUB_DEVICE_CONNECTION_STRING = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

class Database:
    def __init__(self) -> None:
        pass

    def connect(self) -> tuple[object, object]:
        """Connect to the database, return obj:connection, obj:cursor"""
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, connect_timeout=60)
        cursor = connection.cursor()
        return connection, cursor
    
    def close(self, connection) -> None:
        """Close the database connection"""
        connection.close()

    def insert(self, device_id, temperature, pressure, humidity, timestamp, sync) -> None:
        """Insert dataset into the local database"""
        try:
            connection, cursor = self.connect()
            query = f"INSERT INTO {DB_TABLE} (device_id, temperature, pressure, humidity, timestamp, sync) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (device_id, temperature, pressure, humidity, timestamp, sync)
            cursor.execute(query, values)
            connection.commit()
        except Exception as e: print(e)
        finally:
            try:
                self.close(connection)
            except Exception: pass

class Mosquitto:
    def __init__(self) -> None:
        self.client = mqtt.Client()
        self.client.on_connect = self.__on_connect
        self.client.on_message = self.__on_message
        self.client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.mqtt_db = Database()
    
    def __on_connect(self, client, userdata, flags, rc) -> None:
        """
        Print the connection status and subscribe to the mqtt topic
        """
        print("Connected with result code " + str(rc))
        self.client.subscribe(MQTT_TOPIC)
    
    def __on_message(self, client, userdata, msg) -> None:
        """
        When a message is received on the mqtt topic:
        * Decode the message.
        * Send the message to Azure IoT Hub.
        * Update the local database.
        """
        message = msg.payload.decode("utf-8")
        print(f"Received message: {message}")
        data = json.loads(message)

        temperature = data.get("Temperature")
        pressure = data.get("Pressure")
        humidity = data.get("Humidity")
        timestamp = data.get("Timestamp")
        device_id = data.get("Device_ID")
        sync = azure.send_message(data)

        self.mqtt_db.insert(
            device_id=device_id,
            temperature=temperature,
            pressure=pressure,
            humidity=humidity,
            timestamp=timestamp,
            sync=sync
        )

    def loop_forever(self) -> None:
        self.client.loop_forever()

class Azure:
    def __init__(self):
        self.client = IoTHubDeviceClient.create_from_connection_string(IOTHUB_DEVICE_CONNECTION_STRING)
    
    def send_message(self, message) -> bool:
        """Send message to Azure IoT Hub"""
        msg = Message(json.dumps(message))
        msg.content_encoding = "utf-8"
        msg.content_type = "application/json"

        try:    # Attempt to send message to Azure IoT Hub, return True if successful else False
            self.client.send_message(msg)
            print("Message sent to Azure IoT Hub")
            return True
        except Exception as e:
            print("Message not sent to Azure IoT Hub: ", e)
            return False

def main():
    """
    Main code block, this block:
    1. retrieves data from the MQTT broker.
    2. attempts to send it to Azure IoT Hub.
    3. updates the local database.
    
    These tasks are all handled by the Mosquitto class.
    """

    mosquitto = Mosquitto()
    mosquitto.loop_forever()

def sync():
    """
    Secondary code block, this block:
    1. checks the local database for unsynchronized data.
    2. attempts to send it to Azure IoT Hub.
    3. updates the local database.
    """

    db = Database()
    while True:
        connection, cursor = db.connect()
        query = f"SELECT * FROM {DB_TABLE} WHERE sync = 0"
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            row_dict = {
                "device_id": row[0],
                "temperature": row[1],
                "pressure": row[2],
                "humidity": row[3],
                "timestamp": row[4].strftime("%Y-%m-%d %H:%M:%S"),
                "sync": row[5]
            }           
            if azure.send_message(row_dict):
                query = f"UPDATE {DB_TABLE} SET sync = 1 WHERE timestamp = '{row[4]}'"
                cursor.execute(query)
                connection.commit()
            else:
                break   # Break ot of for loop in case of failure.

        db.close(connection)
        time.sleep(60)  # Check for unsynced data every 60 seconds
        
if __name__ == "__main__":
    azure = Azure()

    main_thread = Thread(target=main)
    sync_thread = Thread(target=sync)

    try:
        main_thread.start()
        sync_thread.start()
    except KeyboardInterrupt:
        pass
    finally:
        main_thread.join()
        sync_thread.join()