import hashlib
import os

def hash_ip(ip: str, salt: str = None) -> str:
    """Hash an IP address with optional salt for privacy"""
    if not ip:
        return "unknown"
    
    # Use provided salt or get from environment
    if salt is None:
        salt = os.getenv("ANALYTICS_SALT", "")
    
    # Add salt and hash
    salted = ip + salt
    return hashlib.sha256(salted.encode()).hexdigest()
