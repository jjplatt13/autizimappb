from app.repositories.base import BaseRepository
from schemas.provider import Provider


class ProviderRepository(BaseRepository):
    """
    Repository for provider search queries.
    """

    def search_basic(self, query: str, limit: int = 50):
        sql = """
            SELECT *
            FROM providers
            WHERE name ILIKE %s
            ORDER BY name
            LIMIT %s
        """
        rows = self.fetchall(sql, (f"%{query}%", limit))
        return [Provider(**row) for row in rows]

    def search_fuzzy(self, query: str, limit: int = 50):
        sql = """
            SELECT *
            FROM providers
            WHERE similarity(name, %s) > 0.3
            ORDER BY similarity(name, %s) DESC
            LIMIT %s
        """
        rows = self.fetchall(sql, (query, query, limit))
        return [Provider(**row) for row in rows]

    def search_nearby(
        self,
        lat: float,
        lon: float,
        radius_meters: float,
        limit: int = 50,
    ):
        """
        Spatial nearby search using PostGIS.
        Uses `location` column.
        """

        sql = """
            SELECT *,
                   ST_Distance(
                       location::geography,
                       ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                   ) AS distance
            FROM providers
            WHERE location IS NOT NULL
              AND ST_DWithin(
                    location::geography,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                    %s
              )
            ORDER BY distance
            LIMIT %s
        """

        rows = self.fetchall(
            sql,
            (
                lon,
                lat,
                lon,
                lat,
                radius_meters,
                limit,
            ),
        )

        return [Provider(**row) for row in rows]
