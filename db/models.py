from pydantic import BaseModel
from typing import Optional

class Provider(BaseModel):
    id: int
    name: str
    phone: Optional[str]
    email: Optional[str]
    website: Optional[str]
    street: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip: Optional[str]
    full_address: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    services: Optional[str]
    distance_miles: Optional[float] = None
