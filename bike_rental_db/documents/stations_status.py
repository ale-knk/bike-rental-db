from pydantic import Field
from datetime import datetime
from bson import ObjectId
from pymongokit.documents import BaseDocument

class StationStatusDocument(BaseDocument):
    station: ObjectId = Field(..., description="MongoDB ID of the station")
    bikes_available: int = Field(..., ge=0, description="Number of bikes available at the station")
    docks_available: int = Field(..., ge=0, description="Number of docks available at the station")
    time: datetime = Field(..., description="Timestamp of the status")
