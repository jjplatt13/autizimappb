import time
import uuid
from typing import Optional, Dict, Any, List
from app.utils.redis_client import redis_client
from app.utils.hashing_service import hash_ip
from app.utils.device_parser import parse_device_type


# ----------------------------------------
# Keyword Dictionaries
# ----------------------------------------

URGENCY_KEYWORDS = ["urgent", "asap", "immediately", "emergency", "now"]
QUALITY_KEYWORDS = ["best", "top", "highest rated", "recommended"]
COST_KEYWORDS = ["cheap", "affordable", "low cost", "accepts insurance", "sliding scale"]

SERVICE_KEYWORDS = {
    "aba": ["aba", "behavior therapy"],
    "speech": ["speech", "slp", "speech therapy"],
    "ot": ["ot", "occupational therapy"]
}

COMMON_MISSPELLINGS = {
    "autsim": "autism",
    "abe therapy": "aba therapy",
    "spech": "speech",
    "therpay": "therapy",
    "behavour": "behavior"
}


# ----------------------------------------
# Helper Functions
# ----------------------------------------

def clean_query(raw: str) -> str:
    q = raw.lower().strip()
    for wrong, correct in COMMON_MISSPELLINGS.items():
        if wrong in q:
            q = q.replace(wrong, correct)
    return q


def detect_misspellings(raw: str) -> Dict[str, Any]:
    detected = False
    corrected = raw.lower()

    for wrong, correct in COMMON_MISSPELLINGS.items():
        if wrong in corrected:
            detected = True
            corrected = corrected.replace(wrong, correct)

    return {
        "detected": detected,
        "corrected": corrected
    }


def detect_intent(clean_q: str) -> Dict[str, Any]:
    q = clean_q.lower()

    return {
        "urgency": any(word in q for word in URGENCY_KEYWORDS),
        "quality": any(word in q for word in QUALITY_KEYWORDS),
        "cost_sensitive": any(word in q for word in COST_KEYWORDS),

        "cross_service": (
            ("aba" in q and "speech" in q) or
            ("aba" in q and "ot" in q) or
            ("speech" in q and "ot" in q)
        ),

        "complexity_score": len(q.split())
    }


# ----------------------------------------
# Main Analytics Event Builder
# ----------------------------------------

def build_analytics_event(
    *,
    raw_query: str,
    search_type: str,
    result_count: int,
    ip_addr: str,
    headers: Dict[str, Any],
    geo: Optional[Dict[str, Any]] = None,
    response_ms: Optional[int] = None,
    session_id: Optional[str] = None
) -> Dict[str, Any]:

    if session_id is None:
        session_id = str(uuid.uuid4())

    clean_q = clean_query(raw_query)
    misspell_info = detect_misspellings(raw_query)
    intent = detect_intent(clean_q)

    device = parse_device_type(headers.get("User-Agent", ""))

    event = {
        "session_id": session_id,
        "timestamp": int(time.time()),
        "search_type": search_type,  # basic | fuzzy | nearby

        "query": raw_query,
        "clean_query": clean_q,

        "intent": intent,
        "misspelling": misspell_info,

        "result_count": result_count,
        "response_ms": response_ms or 0,
        "ip_hash": hash_ip(ip_addr),
        "device": device,
    }

    if geo:
        event["geo"] = geo

    return event


# ----------------------------------------
# Send to Redis Stream
# ----------------------------------------

def push_analytics_event(event: Dict[str, Any]):
    try:
        redis_client.xadd("analytics_stream", event)
    except Exception as e:
        # Sentry will capture this in main.py
        print("Analytics push failed:", e)
