from pydantic import Field
from bson import ObjectId
from datetime import datetime
from pymongokit.documents import BaseDocument

class TripDocument(BaseDocument):
    start_station: ObjectId = Field(None, description="ID of the start station")
    end_station: ObjectId = Field(None, description="ID of the end station")
    start_date: datetime = Field(None, description="Start date and time of the trip")
    end_date: datetime = Field(None, description="End date and time of the trip")
    start_time_norm: float = Field(None, description="Normalized hour of starting time")
    end_time_norm: float = Field(None, description="Normalized hour of ending time")
    duration: int = Field(None, description="Duration of the trip in seconds")
    bike_id: int = Field(None, description="ID of the bike used for the trip")
    subscription_type: str = Field(None, max_length=255, description="Type of subscription used for the trip")
    zip_code: str = Field(None, max_length=255, description="ZIP code of the subscriber")
    split: str = Field(None, description="Train, validation, or test split")