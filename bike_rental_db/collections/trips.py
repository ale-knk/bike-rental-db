from bson import ObjectId
from bike_rental_db.connect import db
from bike_rental_db.collections.stations import StationsCollection
from bike_rental_db.documents import TripDocument
from bike_rental_db.data.utils import set_trips_df
from bike_rental_db.error_handler.trips import TripsErrorHandler
from copy import deepcopy
from bike_rental_db.pipelines import TripsPipelines
from typing import Callable

from pymongokit.collections import BaseCollection

class TripsCollection(BaseCollection):
    def __init__(self):
        super().__init__(db["trips"], TripDocument)
        self.pipelines = TripsPipelines()
        self.error_handler = TripsErrorHandler()
        self.stations_col = StationsCollection()

    def create_indexes(self):
        self.collection.create_index([("start_station", 1)])
        self.collection.create_index([("end_station", 1)])
        self.collection.create_index([("start_date", 1)])
        self.collection.create_index([("end_date", 1)])
        self.collection.create_index([("duration", 1)])
        self.collection.create_index([("bike_id", 1)])
        self.collection.create_index([("subscription_type", 1)])
        self.collection.create_index([("zip_code", 1)])
        self.collection.create_index([("split", 1)])

    def find(self, 
             query: dict, 
             convert_stations: bool = True,
             pipeline_steps: list = [],
             handle_func: Callable | None = None) -> list[dict]:      
        
        find_query = deepcopy(query)
        start_station_fields = [key for key in query.keys() if "start_station." in key]
        end_station_fields = [key for key in query.keys() if "end_station." in key]

        if start_station_fields:
            query_stations = {key.split(".")[-1]:query[key] for key in start_station_fields}
            stations_docs = self.stations_col.find(query_stations)
            mongoids = [doc["_id"] for doc in stations_docs]
            find_query["start_station"] = {"$in":mongoids}
            for key in start_station_fields:
                find_query.pop(key)

        if end_station_fields:
            query_stations = {key.split(".")[-1]:query[key] for key in end_station_fields}            
            stations_docs = self.stations_col.find(query_stations)
            mongoids = [doc["_id"] for doc in stations_docs]
            find_query["end_station"] = {"$in":mongoids}
            for key in end_station_fields:
                find_query.pop(key)

        return super().find(
            query=find_query,
            pipeline_steps = self.pipelines.replace_station_ids_pipeline + pipeline_steps if convert_stations else pipeline_steps,
            handle_func=handle_func
        )
        
    def find_by_id(self, 
                document_id: ObjectId,
                convert_stations: bool = True,
                pipeline_steps : list = [],
                handle_func: Callable | None = None) -> dict | None:

        return super().find_by_id(
            document_id=document_id,
            pipeline_steps = self.pipelines.replace_station_ids_pipeline + pipeline_steps if convert_stations else pipeline_steps,
            handle_func=handle_func
        )
    
    def find_aggregated_trips(
            self,
            query: dict = {},
            freq: str = "daily",
            by_station: bool = False) -> list[dict]:
        
        pipeline = []
        pipeline.append({"$match": query})

        if freq == "daily":
            if by_station:
                post_query =self.pipelines.daily_trips_station_pipe
            else:
                post_query =self.pipelines.daily_trips_pipe
        elif freq == "monthly":
            if by_station:
                post_query = self.pipelines.monthly_trips_station_pipe
            else:
                post_query = self.pipelines.monthly_trips_pipe

        pipeline += post_query
        
        return self.find(
            query=query,
            pipeline_steps = pipeline,
            convert_stations=False
        )
    
    def populate(self):
        self.collection.drop()
        self.create_indexes()
        self.delete_many({})
        trips_df = set_trips_df()
        trips_df = trips_df[["start_station_mongoid","end_station_mongoid","start_date","end_date","start_time_norm","end_time_norm","duration","zip_code","subscription_type","bike_id","split"]]
        trips_df.rename(columns={"start_station_mongoid":"start_station","end_station_mongoid":"end_station"}, inplace=True)
        trips_documents = trips_df.to_dict(orient="records")

        self.insert_many(docs=trips_documents)
