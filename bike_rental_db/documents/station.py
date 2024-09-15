from pydantic import Field
from datetime import datetime
from pymongokit.documents import BaseDocument

class StationDocument(BaseDocument):
    id: int = Field(..., ge=0, le=69, description="ID of the station (must be between 0 and 69)")
    id_unmapped: int = Field(..., ge=2, le=84, description="Original ID of the station before preprocessing (must be between 2 and 84)")
    name: str = Field(..., max_length=255, description="Name of the station")
    long: float = Field(..., description="Longitude of the station")
    lat: float = Field(..., description="Latitude of the station")
    dock_count: int = Field(..., ge=0, description="Number of docks at the station")
    city: str = Field(..., max_length=255, description="City where the station is located")
    cluster: int = Field(..., ge=0, description="Cluster number of the station")
    installation_date: datetime = Field(..., description="Date when the station was installed")
