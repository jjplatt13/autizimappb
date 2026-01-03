from pydantic import BaseModel
from typing import Optional, List


class ProviderViewEvent(BaseModel):
    provider_id: int
    session_id: str
    ip_hash: str
    device: str
    timestamp: int
    from_search_query: Optional[str] = None
    position_in_results: Optional[int] = None


class ProviderConversionEvent(BaseModel):
    provider_id: int
    session_id: str
    ip_hash: str
    device: str
    timestamp: int
    conversion_type: str


class ProviderComparisonEvent(BaseModel):
    provider_ids: List[int]
    session_id: str
    ip_hash: str
    device: str
    timestamp: int
