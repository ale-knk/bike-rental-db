from bike_rental_db.connect import db
from bike_rental_db.documents import StationDocument
from bike_rental_db.data.utils import set_stations_df

from pymongokit.collections import BaseCollection

class StationsCollection(BaseCollection):
    def __init__(self):
        super().__init__(db["stations"], StationDocument)
        
    def create_indexes(self):
        self.collection.create_index([("station_id", 1)])
        self.collection.create_index([("end_station_mongoid", 1)])

    def populate(self):
        self.create_indexes()
        self._execute_with_error_handling(
            func=self.delete_many,
            error_handler=self.error_handler,
            query={}
        )
        stations_df = set_stations_df()
        stations_documents = stations_df.to_dict(orient="records")
        validated_documents = self._validate_docs(stations_documents)

        return self._execute_with_error_handling(
            func=self.insert_many,
            error_handler=self.error_handler,
            docs=[doc.dict() for doc in validated_documents]
        )

