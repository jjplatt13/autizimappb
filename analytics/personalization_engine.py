"""
Personalization Engine - Calculates personalization scores
Location: analytics/personalization_engine.py
"""

from typing import Optional, Dict
from db.connection import get_db
from psycopg2.extras import RealDictCursor


def calculate_personalization_score(
    session_id: str,
    user_id: Optional[int] = None
) -> Dict[str, any]:
    """
    Calculate personalization score based on user history
    
    Analyzes user's past behavior to personalize future results
    
    Args:
        session_id: Current session ID
        user_id: User ID if authenticated
        
    Returns:
        Dictionary with personalization data:
        {
            'preferred_services': ['ABA', 'Speech'],
            'preferred_locations': ['Miami', 'Orlando'],
            'avg_search_radius': 15,
            'engagement_score': 0.75
        }
    """
    
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get user's search history
        if user_id:
            cur.execute("""
                SELECT metadata, event_type
                FROM user_activity
                WHERE user_id = %s
                ORDER BY timestamp DESC
                LIMIT 50
            """, (user_id,))
        else:
            cur.execute("""
                SELECT metadata, event_type
                FROM user_activity
                WHERE session_id = %s
                ORDER BY timestamp DESC
                LIMIT 20
            """, (session_id,))
        
        events = cur.fetchall()
        conn.close()
        
        if not events:
            return {
                'preferred_services': [],
                'preferred_locations': [],
                'avg_search_radius': 25,
                'engagement_score': 0.0
            }
        
        # Analyze search patterns
        searches = [e for e in events if e['event_type'] in ('search', 'fuzzy_search')]
        nearby_searches = [e for e in events if e['event_type'] == 'nearby_search']
        
        # Extract preferred services from search queries
        preferred_services = []
        for search in searches:
            if search.get('metadata'):
                query = search['metadata'].get('query', '').lower()
                if 'aba' in query:
                    preferred_services.append('ABA')
                if 'speech' in query:
                    preferred_services.append('Speech')
                if 'ot' in query or 'occupational' in query:
                    preferred_services.append('OT')
        
        # Calculate average search radius
        radii = [s['metadata'].get('radius', 25) for s in nearby_searches if s.get('metadata')]
        avg_radius = sum(radii) / len(radii) if radii else 25
        
        # Calculate engagement score based on activity
        high_intent_events = [e for e in events if e['event_type'] in ('provider_view', 'phone_click', 'website_click')]
        engagement_score = min(len(high_intent_events) / 10.0, 1.0)
        
        return {
            'preferred_services': list(set(preferred_services)),
            'preferred_locations': [],
            'avg_search_radius': int(avg_radius),
            'engagement_score': round(engagement_score, 2)
        }
        
    except Exception as e:
        print(f"Error calculating personalization: {e}")
        return {
            'preferred_services': [],
            'preferred_locations': [],
            'avg_search_radius': 25,
            'engagement_score': 0.0
        }
