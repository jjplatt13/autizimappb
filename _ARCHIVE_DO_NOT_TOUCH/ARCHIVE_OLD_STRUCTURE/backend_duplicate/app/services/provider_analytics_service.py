import time
import uuid
from typing import Dict, Any, Optional, List

from app.utils.redis_client import redis_client
from app.utils.hashing_service import hash_ip
from app.utils.device_parser import parse_device_type


# ----------------------------------------------------
# Provider View Event (Phase 2A)
# ----------------------------------------------------

def build_provider_view_event(
    *,
    provider_id: int,
    ip_addr: str,
    headers: Dict[str, Any],
    session_id: Optional[str] = None,
    from_search_query: Optional[str] = None,
    position_in_results: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Fired whenever a provider profile is viewed.
    Used for:
      - repeat view tracking
      - hot lead detection
      - provider interest analytics
    """
    if session_id is None:
        session_id = str(uuid.uuid4())

    device = parse_device_type(headers.get("User-Agent", ""))

    event: Dict[str, Any] = {
        "event_type": "provider_view",
        "timestamp": int(time.time()),

        "provider_id": provider_id,
        "session_id": session_id,
        "ip_hash": hash_ip(ip_addr),
        "device": device,

        "from_search_query": from_search_query,
        "position_in_results": position_in_results,
    }

    return event


# ----------------------------------------------------
# Provider Conversion Event (Phase 2B)
# ----------------------------------------------------

def build_provider_conversion_event(
    *,
    provider_id: int,
    conversion_type: str,  # "phone" | "website" | "email" | "directions"
    ip_addr: str,
    headers: Dict[str, Any],
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fired whenever a user clicks a high-intent action on a provider:
      - phone call
      - website
      - email
      - directions

    This is the backbone for:
      - pay-per-lead
      - provider ROI dashboards
      - funnel analytics
    """
    if session_id is None:
        session_id = str(uuid.uuid4())

    device = parse_device_type(headers.get("User-Agent", ""))

    event: Dict[str, Any] = {
        "event_type": "provider_conversion",
        "timestamp": int(time.time()),

        "provider_id": provider_id,
        "session_id": session_id,
        "ip_hash": hash_ip(ip_addr),
        "device": device,
        "conversion_type": conversion_type,
    }

    return event


# ----------------------------------------------------
# Provider Comparison Event (future: comparison chains)
# ----------------------------------------------------

def build_comparison_event(
    *,
    provider_ids: List[int],
    session_id: str,
    ip_addr: str,
    headers: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Fired when we detect the same user comparing multiple providers
    within a short timeframe.
    """
    device = parse_device_type(headers.get("User-Agent", ""))

    event: Dict[str, Any] = {
        "event_type": "provider_comparison",
        "timestamp": int(time.time()),

        "provider_ids": provider_ids,
        "session_id": session_id,
        "ip_hash": hash_ip(ip_addr),
        "device": device,
    }

    return event


# ----------------------------------------------------
# Common Redis Push
# ----------------------------------------------------

def push_provider_event(event: Dict[str, Any]) -> None:
    """
    Push any provider-related analytics event into the analytics_stream.
    A background worker (analytics_worker.py) will later consume and
    persist / aggregate these.
    """
    try:
        redis_client.xadd("analytics_stream", event)
    except Exception as e:
        # Do NOT break live requests over analytics issues.
        print("Provider analytics push failed:", e)
