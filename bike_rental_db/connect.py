from pymongo import MongoClient
from bike_rental_db.config import Config

def connect_to_db():
    client = MongoClient(Config.MONGODB_URI)
    db = client.get_database("bike_rental")
    return db

db = connect_to_db()