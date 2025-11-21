import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017')
MONGO_DB = os.getenv('MONGO_DB', 'transflow_db')

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
corridas_collection = db['corridas']


def get_mongo_collection():
    return corridas_collection
