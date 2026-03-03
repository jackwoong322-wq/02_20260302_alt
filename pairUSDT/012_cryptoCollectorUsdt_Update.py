"""
암호화폐 OHLCV 업데이트 - USDT 페어

코인 목록:
  - DB의 coins 테이블에서 조회

데이터 업데이트:
  - ohlcv 테이블의 코인별 마지막 날짜 조회
  - 마지막 날짜 다음날부터 오늘까지 Binance에서 증분 수집

DB: lib.common.config.DB_PATH (pairUSDT/crypto_usdt.db)
"""

import sqlite3
import time
import logging
from datetime import datetime, timedelta, timezone

import requests

from lib.common.config import DB_PATH

# ── 설정 ──────────────────────────────────────────────
BINANCE_QUOTE = "USDT"
BINANCE_DELAY = 0.2
MAX_RETRIES   = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("collector_usdt.log"),
        logging.StreamHandler()
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════
# DB
# ══════════════════════════════════════════════════════

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS coins (
            id         TEXT PRIMARY KEY,
            symbol     TEXT NOT NULL,
            name       TEXT NOT NULL,
            rank       INTEGER,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS ohlcv (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id      TEXT    NOT NULL,
            date         TEXT    NOT NULL,
            open         REAL    NOT NULL,
            high         REAL    NOT NULL,
            low          REAL    NOT NULL,
            close        REAL    NOT NULL,
            volume_base  REAL    NOT NULL DEFAULT 0,
            volume_quote REAL    NOT NULL DEFAULT 0,
            trade_count  INTEGER NOT NULL DEFAULT 0,
            source       TEXT,
            UNIQUE(coin_id, date),
            FOREIGN KEY (coin_id) REFERENCES coins(id)
        );

        CREATE INDEX IF NOT EXISTS idx_ohlcv_coin_date ON ohlcv(coin_id, date);
    """)
    conn.commit()
    log.info("DB 초기화 완료")


def get_coins_from_db(conn: sqlite3.Connection) -> list[dict]:
    """coins 테이블에서 전체 코인 목록 조회 (rank 순)"""
    rows = conn.execute(
        "SELECT id, symbol, name, rank FROM coins ORDER BY rank"
    ).fetchall()
    return [{"id": r[0], "symbol": r[1], "name": r[2], "rank": r[3]} for r in rows]


def get_last_date(conn: sqlite3.Connection, coin_id: str) -> str | None:
    """ohlcv 테이블에서 해당 코인의 가장 마지막 날짜 반환. 데이터 없으면 None."""
    row = conn.execute(
        "SELECT MAX(date) FROM ohlcv WHERE coin_id = ?", (coin_id,)
    ).fetchone()
    return row[0] if row and row[0] else None


# ══════════════════════════════════════════════════════
# 공통 유틸
# ══════════════════════════════════════════════════════

def ts_to_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

def date_to_ts_ms(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def next_date(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    return dt.strftime("%Y-%m-%d")

def today_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

def api_get(url: str, params: dict = None, retries: int = MAX_RETRIES) -> dict | list | None:
    for attempt in range(1, retries + 1):
        try:
            res = requests.get(url, params=params, timeout=30)
            if res.status_code == 200:
                return res.json()
            elif res.status_code == 429:
                wait = 60 * attempt
                log.warning(f"Rate limit! {wait}초 대기 후 재시도...")
                time.sleep(wait)
            else:
                log.error(f"HTTP {res.status_code} | {url} | {res.text[:200]}")
                return None
        except requests.RequestException as e:
            log.error(f"요청 오류 (시도 {attempt}/{retries}): {e}")
            time.sleep(5 * attempt)
    return None


# ══════════════════════════════════════════════════════
# Binance OHLCV
# ══════════════════════════════════════════════════════

def binance_fetch_klines(symbol: str, from_date: str | None = None) -> list[list]:
    """
    Binance에서 일봉 kline 수집.
    from_date: 'YYYY-MM-DD'. 지정 시 해당 날짜부터 오늘까지 수집.
               None이면 상장일부터 전체 수집.
    """
    url        = "https://api.binance.com/api/v3/klines"
    start_time = date_to_ts_ms(from_date) if from_date else 0
    all_klines = []

    while True:
        data = api_get(url, {
            "symbol":    f"{symbol}{BINANCE_QUOTE}",
            "interval":  "1d",
            "startTime": start_time,
            "limit":     1000,
        })
        time.sleep(BINANCE_DELAY)

        if not data:
            break

        all_klines.extend(data)

        if len(data) < 1000:
            break

        start_time = data[-1][6] + 1

    return all_klines

def parse_binance_klines(klines: list[list]) -> list[dict]:
    return [{
        "date":         ts_to_date(k[0]),
        "open":         float(k[1]),
        "high":         float(k[2]),
        "low":          float(k[3]),
        "close":        float(k[4]),
        "volume_base":  float(k[5]),
        "volume_quote": float(k[7]),
        "trade_count":  int(k[8]),
        "source":       "binance",
    } for k in klines]


# ══════════════════════════════════════════════════════
# DB 저장
# ══════════════════════════════════════════════════════

def save_rows(conn: sqlite3.Connection, coin_id: str, rows: list[dict]) -> int:
    data = [(
        coin_id,
        r["date"], r["open"], r["high"], r["low"], r["close"],
        r["volume_base"], r["volume_quote"], r["trade_count"], r["source"],
    ) for r in rows]

    conn.executemany("""
        INSERT OR IGNORE INTO ohlcv
            (coin_id, date, open, high, low, close,
             volume_base, volume_quote, trade_count, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()
    return conn.execute("SELECT changes()").fetchone()[0]


# ══════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════

def main():
    today = today_utc()

    log.info("=" * 55)
    log.info("암호화폐 OHLCV 업데이트 시작 (USDT 페어)")
    log.info(f"  DB   : {DB_PATH}")
    log.info(f"  오늘 : {today}")
    log.info("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # ── DB에서 코인 목록 조회 ────────────────────────
    coins = get_coins_from_db(conn)

    if not coins:
        log.error("coins 테이블에 데이터가 없습니다. 먼저 코인 목록을 등록하세요.")
        conn.close()
        return

    log.info(f"\n업데이트 대상: {len(coins)}개 코인\n")

    # ── 코인별 증분 업데이트 ─────────────────────────
    skipped = 0
    updated = 0

    for i, coin in enumerate(coins, 1):
        coin_id   = coin["id"]
        symbol    = coin["symbol"]
        last_date = get_last_date(conn, coin_id)

        log.info(f"[{i}/{len(coins)}] {symbol} ({coin_id})")

        if not last_date:
            log.warning(f"  ohlcv 데이터 없음 → 건너뜀")
            skipped += 1
            continue

        from_date = next_date(last_date)

        if from_date > today:
            log.info(f"  이미 최신 상태 (마지막: {last_date}) → 건너뜀")
            skipped += 1
            continue

        log.info(f"  마지막: {last_date} → {from_date} ~ {today} 업데이트 중...")
        klines = binance_fetch_klines(symbol, from_date=from_date)

        if not klines:
            log.warning(f"  Binance {symbol}USDT 수신 데이터 없음")
            skipped += 1
            continue

        rows  = parse_binance_klines(klines)
        rows  = [r for r in rows if r["date"] < today]   # 미완성 캔들 제외

        if not rows:
            log.info(f"  저장할 새 데이터 없음")
            skipped += 1
            continue

        saved = save_rows(conn, coin_id, rows)
        log.info(f"  +{saved}일치 저장 ({rows[0]['date']} ~ {rows[-1]['date']})")
        updated += 1

    conn.close()
    log.info("\n" + "=" * 55)
    log.info(f"업데이트 완료! DB: {DB_PATH}")
    log.info(f"  업데이트: {updated}개 코인")
    log.info(f"  건너뜀  : {skipped}개 코인")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
