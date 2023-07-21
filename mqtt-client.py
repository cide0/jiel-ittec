import paho.mqtt.client as mqtt
import mysql.connector as mysql
from pymongo import MongoClient
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime

#sql
db = mysql.connect(
    host="localhost",
    database="jiel",
    user="root",
    password=""
)

#mongoDB
CONNECTION_STRING = "mongodb://localhost:27017"
client = MongoClient(CONNECTION_STRING)
mongo_db = client['jiel']

#influx
token = "hVROOZx31H_bnByj0e5I_ouiiRRopJnDzzND76d1Ugq6-nm7fKYF1TV4lj2z_3fTBQZf6atolACwW1pJ4LC21g=="
org = "jiel"
bucket = "jiel_ittec"

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("esp32/dht/gruppe 2/temperature elias")
    client.subscribe("esp32/dht/gruppe 2/humidity elias")
    client.subscribe("esp32/dht/gruppe 2/temperature Jack")
    client.subscribe("esp32/dht/gruppe 2/humidity Jack")


def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    espName = ''
    if 'elias' in msg.topic:
        espName = 'elias'
    elif 'Jack' in msg.topic:
        espName = 'jack'

    dataType = ''
    if 'temperature' in msg.topic:
        dataType = 'temperature'
    elif 'humidity' in msg.topic:
        dataType = 'humidity'

    val = [float(msg.payload)]

#mySQL
    stmt = db.cursor()
    tableName = dataType + '_' + espName
    query = "INSERT INTO " + tableName + " (" + dataType + ") VALUES (%s)"
    stmt.execute(query, val)
    db.commit()

#mongoDB
    mongo_entry = {
        "type": dataType,
        "value": val[0],
        "timestamp": datetime.datetime.now()
    }
    collection = mongo_db["data_" + espName]
    collection.insert_one(mongo_entry)

#influx
    with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as influxClient:
        write_api = influxClient.write_api(write_options=SYNCHRONOUS)

        data = "esp32Data,type=" + dataType + " value=" + str(val[0])
        write_api.write(bucket, org, data)


client = mqtt.Client()
client.username_pw_set("JIEL", "15591559")
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.105.145", 1883, 60)

client.loop_forever()



