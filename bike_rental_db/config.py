import os
from pathlib import Path
from dotenv import load_dotenv



load_dotenv()
BASE_DIR = Path(__file__).resolve().parent

class Config:
    MONGODB_URI = os.getenv('MONGODB_URI')
    STATIONS_CSV_PATH = os.getenv("STATIONS_CSV_PATH", BASE_DIR / 'data/stations.csv')
    TRIPS_CSV_PATH = os.getenv("TRIPS_CSV_PATH", BASE_DIR / 'data/trips.csv')
    STATUS_CSV_PATH = os.getenv("STATUS_CSV_PATH", BASE_DIR / 'data/status.csv')