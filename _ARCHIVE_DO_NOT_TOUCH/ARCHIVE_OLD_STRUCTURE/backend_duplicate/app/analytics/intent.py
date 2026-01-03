"""
Intent Scoring Engine
Scores user actions for lead qualification and revenue potential
"""

def score_intent(event_type: str, metadata: dict) -> float:
    """
    Calculate intent score (0.0 - 1.0) based on event type and metadata
    Higher scores = higher purchase/conversion intent
    """
    
    base_scores = {
        "search": 0.3,
        "fuzzy_search": 0.4,
        "nearby_search": 0.6,
        "provider_view": 0.7,
        "provider_list": 0.2,
        "phone_click": 0.95,
        "website_click": 0.85,
        "directions_click": 0.9,
        "email_click": 0.8,
        "favorite_save": 0.75,
    }
    
    score = base_scores.get(event_type, 0.1)
    
    # Boost for urgency keywords
    query = metadata.get("query", "").lower()
    if any(word in query for word in ["asap", "urgent", "emergency", "immediate", "now"]):
        score += 0.2
    
    # Boost for quality keywords
    if any(word in query for word in ["best", "top", "rated", "reviews"]):
        score += 0.1
    
    # Boost for specific service types (high-value)
    if any(word in query for word in ["aba", "bcba", "board certified"]):
        score += 0.15
    
    # Penalize for price sensitivity
    if any(word in query for word in ["cheap", "affordable", "free"]):
        score -= 0.1
    
    # Boost for repeat views (from metadata)
    if metadata.get("repeat_view"):
        score += 0.15
    
    # Boost for fast action (viewed within 5 seconds of search)
    if metadata.get("response_ms", 99999) < 5000:
        score += 0.05
    
    # Clamp between 0 and 1
    return max(0.0, min(1.0, score))


def classify_intent_tier(score: float) -> str:
    """Classify intent score into revenue tiers"""
    if score >= 0.8:
        return "hot_lead"  # $100+ value
    elif score >= 0.6:
        return "warm_lead"  # $50 value
    elif score >= 0.4:
        return "qualified"  # $20 value
    else:
        return "cold"  # $0-5 value
