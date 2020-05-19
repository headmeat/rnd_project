import csv
#import pymongo
from pymongo import MongoClient

client = MongoClient('localhost:27017')
collection=client.cpmongo_distinct.CPS_RATING

with open('/root/gjf.csv') as f:
     reader=csv.DictReader(f)
     for row in reader:
         res = dict(row)
         x = collection.insert_one(res)
         print(res)
