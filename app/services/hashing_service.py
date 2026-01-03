import hashlib
from config.settings import ANALYTICS_SALT

def hash_ip(ip: str) -> str:
    """Hash IP address for privacy-preserving analytics"""
    if not ip:
        return None
    raw = ip + ANALYTICS_SALT
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
