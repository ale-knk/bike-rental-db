from bson import ObjectId
from pymongokit.collections import BaseCollection
from bike_rental_db.connect import db
from bike_rental_db.documents import GroupedTripDocument
from bike_rental_db.collections import TripsCollection, StationsCollection
from bike_rental_db.pipelines import TripsPipelines, GroupedTripsPipelines
from copy import deepcopy
from typing import Callable


class GroupedTripsCollection(BaseCollection):
    def __init__(self):
        super().__init__(db["grouped_trips"], GroupedTripDocument)
        self.pipelines = GroupedTripsPipelines()
        self.stations_coll = StationsCollection()
        self.trips_coll = TripsCollection()
        self.trips_pipelines = TripsPipelines()

    def create_indexes(self):
        self.collection.create_index([("date", 1)])
        self.collection.create_index([("station", 1)])
        self.collection.create_index([("n_trips_start", 1)])  
        self.collection.create_index([("n_trips_end", 1)])  

    def find(self, 
             query: dict, 
             convert_stations: bool = True,
             pipeline_steps: list = [],
             handle_func: Callable | None = None) -> list[dict]:      
        
        find_query = deepcopy(query)
        station_fields = [key for key in query.keys() if "station." in key]

        if station_fields:
            query_stations = {key.split(".")[-1]:query[key] for key in station_fields}
            stations_docs = self.stations_coll.find(query_stations)
            mongoids = [doc["_id"] for doc in stations_docs]
            find_query["station"] = {"$in":mongoids}
            for key in station_fields:
                find_query.pop(key)

        return super().find(
            query=find_query,
            pipeline_steps = self.pipelines.convert_station_id_pipeline + pipeline_steps if convert_stations else pipeline_steps,
            handle_func=handle_func
        )
        
    def find_by_id(self, 
                document_id: ObjectId,
                convert_stations: bool = True,
                pipeline_steps : list = [],
                handle_func: Callable | None = None) -> dict | None:

        return super().find_by_id(
            document_id=document_id,
            pipeline_steps = self.pipelines.convert_stations_pipe + pipeline_steps if convert_stations else pipeline_steps,
            handle_func=handle_func
        )
    
    def populate(self):
        self.collection.drop()
        self.create_indexes()
        self.delete_many({})
        docs = self.trips_coll.find(
            query={},
            pipeline_steps=self.trips_pipelines.group_trips_pipeline,
            convert_stations=False
        )
        self.insert_many(docs=docs)
        self.fill_missing_documents()

    def fill_missing_documents(self):
        unique_dates = self.collection.distinct("date")
        unique_stations = self.collection.distinct("station")
        
        documents_to_insert = []  
        for date in unique_dates:
            present_stations = self.collection.distinct("station", {"date": date})            
            missing_stations = set(unique_stations) - set(present_stations)
            
            for station in missing_stations:
                new_document = {
                    "date": date,
                    "station": ObjectId(station),  
                    "n_trips_start": 0,
                    "n_trips_end": 0
                }
                documents_to_insert.append(new_document) 

        if documents_to_insert:
            self.collection.insert_many(documents_to_insert)