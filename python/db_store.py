from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from const import DB_PATH


class AggregateStore:
    def __init__(self, path: str | None = None):
        self._path = str(Path(path or DB_PATH).resolve())

    @property
    def path(self) -> str:
        return self._path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS aggregates (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              sensor_id TEXT NOT NULL,
              window_start_iso TEXT NOT NULL,
              window_end_iso TEXT NOT NULL,
              avg_celsius REAL NOT NULL,
              sample_count INTEGER NOT NULL,
              computed_at_iso TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agg_sensor_computed "
            "ON aggregates (sensor_id, computed_at_iso)"
        )
        self._conn.commit()

    def insert(
        self,
        sensor_id: str,
        window_start_iso: str,
        window_end_iso: str,
        avg_celsius: float,
        sample_count: int,
        computed_at_iso: str,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO aggregates (
                  sensor_id, window_start_iso, window_end_iso,
                  avg_celsius, sample_count, computed_at_iso
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    sensor_id,
                    window_start_iso,
                    window_end_iso,
                    avg_celsius,
                    sample_count,
                    computed_at_iso,
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def get_latest(self, sensor_id: str) -> sqlite3.Row | None:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT * FROM aggregates
                WHERE sensor_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (sensor_id,),
            )
            return cur.fetchone()

    def list_latest_per_sensor(self) -> list[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT a.* FROM aggregates a
                INNER JOIN (
                  SELECT sensor_id, MAX(id) AS max_id
                  FROM aggregates
                  GROUP BY sensor_id
                ) t ON a.sensor_id = t.sensor_id AND a.id = t.max_id
                ORDER BY a.sensor_id
                """
            )
            return list(cur.fetchall())

    def history(
        self,
        sensor_id: str,
        from_iso: str | None,
        to_iso: str | None,
    ) -> list[sqlite3.Row]:
        clauses = ["sensor_id = ?"]
        params: list[str] = [sensor_id]
        if from_iso:
            clauses.append("computed_at_iso >= ?")
            params.append(from_iso)
        if to_iso:
            clauses.append("computed_at_iso <= ?")
            params.append(to_iso)
        where = " AND ".join(clauses)
        with self._lock:
            cur = self._conn.execute(
                f"""
                SELECT * FROM aggregates
                WHERE {where}
                ORDER BY id ASC
                """,
                params,
            )
            return list(cur.fetchall())
