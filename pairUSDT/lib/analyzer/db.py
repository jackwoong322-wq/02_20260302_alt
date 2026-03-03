import logging
import sqlite3

import numpy as np

from lib.common.utils import safe_log1p, signed_log1p

log = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS coin_analysis_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    coin_id             INTEGER NOT NULL,
    symbol              TEXT    NOT NULL,
    coin_rank           INTEGER,
    cycle_number        INTEGER NOT NULL,
    cycle_name          TEXT    NOT NULL,
    box_index           INTEGER NOT NULL,
    phase               TEXT    NOT NULL,
    result              TEXT    NOT NULL,
    start_x             INTEGER NOT NULL,
    end_x               INTEGER NOT NULL,
    hi                  REAL    NOT NULL,
    lo                  REAL    NOT NULL,
    hi_day              INTEGER,
    lo_day              INTEGER,
    duration            INTEGER NOT NULL,
    range_pct           REAL    NOT NULL,
    hi_change_pct       REAL,
    lo_change_pct       REAL,
    gain_pct            REAL,
    norm_hi             REAL,
    norm_lo             REAL,
    norm_range_pct      REAL,
    norm_duration       REAL,
    norm_hi_change_pct  REAL,
    norm_lo_change_pct  REAL,
    norm_gain_pct       REAL,
    is_completed        INTEGER DEFAULT 0,
    is_prediction       INTEGER DEFAULT 0,
    created_at          TEXT    DEFAULT (datetime('now'))
)
"""

INSERT_SQL = """
INSERT INTO coin_analysis_results (
    coin_id, symbol, coin_rank,
    cycle_number, cycle_name,
    box_index, phase, result,
    start_x, end_x, hi, lo, hi_day, lo_day,
    duration, range_pct,
    hi_change_pct, lo_change_pct, gain_pct,
    norm_hi, norm_lo, norm_range_pct, norm_duration,
    norm_hi_change_pct, norm_lo_change_pct, norm_gain_pct,
    is_completed, is_prediction
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


def setup_db(conn: sqlite3.Connection):
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    log.info("테이블 coin_analysis_results 준비 완료")


def insert_zones(
    conn: sqlite3.Connection,
    coin_id: int,
    symbol: str,
    coin_rank: int,
    cycle_number: int,
    cycle_name: str,
    zones: list,
) -> int:
    rows = []
    for zi, z in enumerate(zones):
        rp = z["range_pct"]
        hi = z["hi"]
        lo = z["lo"]
        dur = z["duration"]
        hcp = z.get("hi_change_pct")
        lcp = z.get("lo_change_pct")
        gp = z.get("gain_pct")

        rows.append(
            (
                coin_id,
                symbol,
                coin_rank,
                cycle_number,
                cycle_name,
                zi,
                z["phase"],
                z["result"],
                z["start_x"],
                z["end_x"],
                hi,
                lo,
                z.get("hi_day"),
                z.get("lo_day"),
                dur,
                rp,
                hcp,
                lcp,
                gp,
                safe_log1p(hi),
                safe_log1p(lo),
                safe_log1p(rp),
                safe_log1p(dur),
                signed_log1p(hcp) if hcp is not None else None,
                signed_log1p(lcp) if lcp is not None else None,
                signed_log1p(gp) if gp is not None else None,
                1 if z["result"] != "ACTIVE" else 0,
                0,
            )
        )

    conn.executemany(INSERT_SQL, rows)
    conn.commit()
    return len(rows)


def load_all_coins(conn: sqlite3.Connection) -> list:
    return conn.execute(
        """
        SELECT c.id, c.symbol, c.name, c.rank
        FROM coins c
        WHERE EXISTS (SELECT 1 FROM alt_cycle_data a WHERE a.coin_id = c.id)
        ORDER BY c.rank
        """
    ).fetchall()


def load_cycle_data(conn: sqlite3.Connection, coin_id: int) -> dict:
    rows = conn.execute(
        """
        SELECT cycle_number, cycle_name, days_since_peak,
               close_rate, high_rate, low_rate,
               peak_date, peak_price, timestamp
        FROM alt_cycle_data
        WHERE coin_id = ?
        ORDER BY cycle_number, days_since_peak
        """,
        (coin_id,),
    ).fetchall()

    cycles: dict = {}
    for row in rows:
        cn = row[0]
        if cn not in cycles:
            cycles[cn] = {
                "cycle_number": cn,
                "cycle_name": row[1],
                "peak_date": row[6],
                "peak_price": row[7],
                "data": [],
            }
        cycles[cn]["data"].append(
            {
                "x": row[2],
                "close": round(row[3], 4),
                "high": round(row[4], 4),
                "low": round(row[5], 4),
                "date": row[8],
            }
        )
    return cycles


def print_norm_stats(label: str, values: list):
    if not values:
        return
    arr = np.array(values, dtype=float)
    log.info(
        "  %-25s min=%.3f  max=%.3f  mean=%.3f  std=%.3f  (n=%d)",
        label,
        arr.min(),
        arr.max(),
        arr.mean(),
        arr.std(),
        len(arr),
    )