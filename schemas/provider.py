from pydantic import BaseModel
from typing import Optional


class Provider(BaseModel):
    id: int
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    full_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    services: Optional[str] = None
    distance_miles: Optional[float] = None

    class Config:
        from_attributes = True
