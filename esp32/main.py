import machine
import BME280
from network import WLAN, STA_IF
from umqttsimple import MQTTClient
import json
import utime
import _thread
import ntptime

"""ENVIRONMENT"""
def get_env(key):
    with open('.env', 'r') as f:
        envs = dict([line.strip().split('=') for line in f.readlines() if line.strip()])
        return envs[key]

MQTT_BROKER = '192.168.2.149'
MQTT_PORT = 1883
MQTT_USER = 'user1'
MQTT_TOPIC = 'esp32/bme280'
WIFI_SSID = get_env('WIFI_SSID')
WIFI_PASS = get_env('WIFI_PASS')
MQTT_PASS = get_env('MQTT_PASS')

"""CONNECT TO WIFI"""
wlan = WLAN(STA_IF)
wlan.active(True)
wlan.connect(WIFI_SSID, WIFI_PASS)
while not wlan.isconnected():
    pass

"""SET TIME"""
ntptime.host = 'nl.pool.ntp.org'
ntptime.settime()

"""CONNECT TO MQTT"""
client = MQTTClient('esp32_client', MQTT_BROKER, port=MQTT_PORT, user=MQTT_USER, password=MQTT_PASS)
client.connect()
print('Connected to %s MQTT broker' % MQTT_BROKER)

"""BME280"""
i2c = machine.I2C(scl=machine.Pin(22), sda=machine.Pin(21))
bme280 = BME280.BME280(i2c=i2c)

def get_bme280_data():
    temperature = bme280.temperature[:-1]   # Remove the trailing 'C' from the temperature string
    humidity = bme280.humidity[:-1]         # Remove the trailing '%' from the humidity string
    pressure = bme280.pressure[:-3]         # Remove the trailing 'hPa' from the pressure string
    timestamp = utime.localtime()
    data = {
        'Temperature': temperature,
        'Humidity': humidity,
        'Pressure': pressure,
        # YYYY-MM-DD HH:MM:SS
        'Timestamp': '%04d-%02d-%02d %02d:%02d:%02d' % (timestamp[0], timestamp[1], timestamp[2], timestamp[3], timestamp[4], timestamp[5])
    }
    return json.dumps(data)

def publish_data():
    while True:
        bme280_data = get_bme280_data()
        client.publish(MQTT_TOPIC, bme280_data) 
        print(bme280_data)
        utime.sleep(10)

_thread.start_new_thread(publish_data, ())