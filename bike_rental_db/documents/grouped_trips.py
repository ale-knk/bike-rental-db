import requests
from datetime import datetime
from pydantic import Field
from bson import ObjectId
from pydantic import Field
from datetime import datetime
from pymongokit.documents import BaseDocument

class GroupedTripDocument(BaseDocument):
    date: datetime = Field(..., description="Date of the grouped trips")
    station: ObjectId = Field(..., description="Station from which the trips originated")
    n_trips_start: int = Field(..., description="Number of trips that started in this date and station.")
    n_trips_end: int = Field(..., description="Number of trips that ended in this date and station.")
