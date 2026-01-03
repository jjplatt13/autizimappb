"""
Intent Scoring Model - Scores user intent based on behavior
Location: analytics/intent_model.py
"""


def score_intent(event_type: str, metadata: dict) -> float:
    """
    Score user intent based on event type and metadata
    
    Higher scores indicate higher purchase/conversion intent
    
    Args:
        event_type: Type of event (search, provider_view, etc.)
        metadata: Event metadata
        
    Returns:
        Intent score (0.0 - 1.0)
    """
    
    base_scores = {
        "provider_list": 0.2,      # Low intent - just browsing
        "search": 0.4,              # Medium intent - actively searching
        "fuzzy_search": 0.5,        # Higher intent - refined search
        "nearby_search": 0.6,       # High intent - location-based
        "provider_view": 0.7,       # Very high intent - viewing details
        "phone_click": 0.95,        # Conversion action
        "website_click": 0.9,       # Conversion action
    }
    
    score = base_scores.get(event_type, 0.3)
    
    # Adjust score based on metadata
    if metadata:
        # More specific searches indicate higher intent
        if event_type == "search" and metadata.get("result_count", 0) < 10:
            score += 0.1
        
        # Fast response times suggest engaged user
        if metadata.get("response_ms", 0) < 100:
            score += 0.05
            
        # Nearby searches with small radius = higher intent
        if event_type == "nearby_search" and metadata.get("radius", 25) <= 10:
            score += 0.1
    
    # Cap at 1.0
    return min(score, 1.0)
