#!/usr/bin/python3.6

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
from json import dumps
from func import getOrigin

consumer = KafkaConsumer(
     'CPS_NCR_PROGRAM_INFO',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
     )

client = MongoClient('localhost:27017')
db = client.cpmongo

CPS_NCR_PROGRAM_INFO = db.CPS_NCR_PROGRAM_INFO

for message in consumer:
    data = getOrigin(message.value)
    CPS_NCR_PROGRAM_INFO.insert_one(data)
    print(f"value={data}")

    


