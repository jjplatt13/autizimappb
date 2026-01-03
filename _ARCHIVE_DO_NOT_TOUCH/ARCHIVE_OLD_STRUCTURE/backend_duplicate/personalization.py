"""
Personalization Engine
Calculate personalization scores based on user behavior
"""
from typing import Dict, Any

def calculate_personalization_score(user_history: list, current_context: dict) -> float:
    """
    Calculate how well current results match user preferences
    Based on past search patterns, location preferences, service types
    
    Returns score 0.0 - 1.0 (higher = better match)
    """
    
    if not user_history:
        return 0.5  # Neutral for new users
    
    score = 0.5
    
    # Prefer services user searched for before
    past_services = [h.get("query", "") for h in user_history if h.get("event_type") == "search"]
    current_query = current_context.get("query", "").lower()
    
    if any(current_query in past.lower() for past in past_services):
        score += 0.2
    
    # Prefer locations user searched before
    past_locations = [(h.get("lat"), h.get("lon")) for h in user_history if h.get("lat")]
    current_location = (current_context.get("lat"), current_context.get("lon"))
    
    if current_location in past_locations:
        score += 0.15
    
    # Prefer providers user viewed before
    if current_context.get("provider_id") in [h.get("provider_id") for h in user_history]:
        score += 0.15
    
    return min(1.0, score)


def get_user_preferences(user_history: list) -> Dict[str, Any]:
    """
    Extract user preferences from history
    Returns dict with preferred services, locations, etc.
    """
    
    preferences = {
        "top_services": [],
        "preferred_locations": [],
        "avg_search_radius": 25,
        "preferred_time": "afternoon"
    }
    
    # Count service searches
    service_counts = {}
    for event in user_history:
        if event.get("event_type") in ["search", "fuzzy_search"]:
            query = event.get("query", "").lower()
            for service in ["aba", "speech", "ot", "pt"]:
                if service in query:
                    service_counts[service] = service_counts.get(service, 0) + 1
    
    preferences["top_services"] = sorted(service_counts, key=service_counts.get, reverse=True)[:3]
    
    # Average radius
    radii = [e.get("radius") for e in user_history if e.get("radius")]
    if radii:
        preferences["avg_search_radius"] = sum(radii) // len(radii)
    
    return preferences
