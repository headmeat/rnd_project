#!/usr/bin/python3.6

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from json import dumps

consumer = KafkaConsumer(
     'V_STD_CDP_SREG',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
     )


client = MongoClient('localhost:27017')
db = client.cpmongo

V_STD_CDP_SREG = db.V_STD_CDP_SREG

for message in consumer:
    #print(V_STD_SREG.dir())
    V_STD_CDP_SREG.insert_one(message.value)
    print(f"value={message.value}")

    


