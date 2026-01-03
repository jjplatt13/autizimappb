"""
Session Management
Cookie-based session tracking with device fingerprinting
"""
import uuid
import hashlib
from fastapi import Request, Response
from typing import Optional

def get_or_create_session(request: Request, response: Response) -> str:
    """
    Get existing session ID from cookie or create new one
    Sets cookie with 1 year expiration
    """
    session_id = request.cookies.get("session_id")
    if not session_id:
        session_id = str(uuid.uuid4())
        response.set_cookie(
            "session_id",
            session_id,
            max_age=60*60*24*365,  # 1 year
            httponly=True,
            samesite="lax"
        )
    return session_id


def get_device_id(request: Request) -> str:
    """
    Generate stable device fingerprint from browser characteristics
    Uses UUID5 for deterministic hashing
    """
    ua = request.headers.get("user-agent", "")
    accept = request.headers.get("accept", "")
    ip = request.client.host if request.client else ""
    
    raw = f"{ua}|{accept}|{ip}"
    return uuid.uuid5(uuid.NAMESPACE_DNS, raw).hex


def hash_ip(ip: str, salt: str) -> Optional[str]:
    """
    Hash IP address for privacy-preserving analytics
    """
    if not ip:
        return None
    return hashlib.sha256((ip + salt).encode()).hexdigest()
