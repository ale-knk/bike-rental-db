from bike_rental_db.connect import db
from bike_rental_db.documents import StationStatusDocument
from bike_rental_db.collections import StationsCollection
from bike_rental_db.data.utils import set_status_df
from bike_rental_db.error_handler.trips import TripsErrorHandler
from bike_rental_db.pipelines import StationStatusPipelines

from pymongokit.collections import BaseCollection

class StationsStatusCollection(BaseCollection):
    def __init__(self):
        super().__init__(db["station_status"], StationStatusDocument)
        self.pipelines = StationStatusPipelines()
        self.error_handler = TripsErrorHandler()
        self.stations_col = StationsCollection()

    def create_indexes(self):
        self.collection.create_index([("station", 1)])
        self.collection.create_index([("time", 1)])
        
    def populate(self):
        self.collection.drop()
        self.create_indexes()
        self.delete_many({})
        status_df = set_status_df()
        status_df = status_df[["station_mongoid","bikes_available","docks_available","time"]]
        status_df.rename(columns={"station_mongoid": "station"}, inplace=True)
        status_documents = status_df.to_dict(orient="records")
        self.insert_many(docs=status_documents)   

    def get_aggregated_status(
            self,
            freq: str = "daily",
            by_station: bool = False,
            query: dict | None = None) -> list:
        pipeline = []

        if query:
            pipeline.append({"$match": query})

        if freq == "daily":
            if by_station:
                pipeline.extend(StationStatusPipelines.daily_status_stations_pipeline)
            else:
                pipeline.extend(StationStatusPipelines.daily_status_pipeline)
        elif freq == "monthly":
            if by_station:
                pipeline.extend(StationStatusPipelines.monthly_status_stations_pipeline)
            else:
                pipeline.extend(StationStatusPipelines.monthly_status_pipeline)
                
        return list(self.collection.aggregate(pipeline))