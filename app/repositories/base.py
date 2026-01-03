from app.core.database import get_db
from psycopg2.extras import RealDictCursor


class BaseRepository:
    """
    Base repository providing DB helpers.

    Guarantees:
    - fetchall() returns List[Dict]
    - fetchone() returns Dict | None
    """

    def __init__(self):
        self._conn = None

    def get_connection(self):
        if self._conn is None:
            self._conn = get_db()
        return self._conn

    def execute(self, query: str, params: tuple | None = None):
        conn = self.get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, params or ())
        return cur

    def fetchall(self, query: str, params: tuple | None = None):
        cur = self.execute(query, params)
        rows = cur.fetchall()
        cur.close()
        return rows

    def fetchone(self, query: str, params: tuple | None = None):
        cur = self.execute(query, params)
        row = cur.fetchone()
        cur.close()
        return row
