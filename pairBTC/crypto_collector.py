"""
암호화폐 데이터 수집기 (Binance + CryptoCompare 병행)

전략:
  1단계 - Binance  → 상장일부터 현재까지 완전한 OHLCV (BTC 페어)
  2단계 - CryptoCompare → Binance 상장 이전 데이터 보완
           DB의 코인별 최초 날짜 확인 후 그 이전 데이터만 가져와 INSERT

데이터 기준: BTC 페어 (예: ETHBTC)
환경변수: .env 파일에서 CC_API_KEY 로드
"""

import os
import sqlite3
import time
import logging
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# ── .env 로드 ──────────────────────────────────────────
load_dotenv()
CC_API_KEY = os.getenv("CC_API_KEY")
if not CC_API_KEY:
    raise ValueError(".env 파일에 CC_API_KEY가 없습니다.")

# ── 설정 ──────────────────────────────────────────────
DB_PATH = "crypto_data.db"
LIST_CURRENCY = "usd"  # CoinGecko 목록 조회용
BINANCE_QUOTE = "BTC"  # Binance 페어 기준
CC_QUOTE = "BTC"  # CryptoCompare 페어 기준
CG_DELAY = 2.5  # CoinGecko rate limit 대응 (초)
BINANCE_DELAY = 0.2  # Binance rate limit 대응 (초)
CC_DELAY = 0.5  # CryptoCompare rate limit 대응 (초)
MAX_RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("collector.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════
# DB
# ══════════════════════════════════════════════════════


def init_db(conn: sqlite3.Connection):
    conn.executescript(
        """
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
            date         TEXT    NOT NULL,     -- YYYY-MM-DD (UTC)
            open         REAL    NOT NULL,
            high         REAL    NOT NULL,
            low          REAL    NOT NULL,
            close        REAL    NOT NULL,
            volume_base  REAL    NOT NULL DEFAULT 0,
            volume_quote REAL    NOT NULL DEFAULT 0,
            trade_count  INTEGER NOT NULL DEFAULT 0,
            source       TEXT,                 -- 'binance' | 'cryptocompare'
            UNIQUE(coin_id, date),
            FOREIGN KEY (coin_id) REFERENCES coins(id)
        );

        CREATE INDEX IF NOT EXISTS idx_ohlcv_coin_date ON ohlcv(coin_id, date);
    """
    )
    conn.commit()
    log.info("DB 초기화 완료")


def get_earliest_date(conn: sqlite3.Connection, coin_id: str) -> str | None:
    """DB에서 코인의 가장 오래된 날짜 조회"""
    row = conn.execute(
        "SELECT MIN(date) FROM ohlcv WHERE coin_id = ?", (coin_id,)
    ).fetchone()
    return row[0] if row else None


# ══════════════════════════════════════════════════════
# 공통 유틸
# ══════════════════════════════════════════════════════


def ts_to_date(ts_ms: int) -> str:
    """밀리초 타임스탬프 → YYYY-MM-DD (UTC)"""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def date_to_ts(date_str: str) -> int:
    """YYYY-MM-DD → 초 단위 타임스탬프 (UTC)"""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def api_get(
    url: str, params: dict = None, retries: int = MAX_RETRIES
) -> dict | list | None:
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
# CoinGecko (코인 목록 조회용)
# ══════════════════════════════════════════════════════


def cg_fetch_top_coins(limit: int = 100) -> list[dict]:
    url = "https://api.coingecko.com/api/v3/coins/markets"
    coins, per_page = [], 100
    pages = (limit + per_page - 1) // per_page

    for page in range(1, pages + 1):
        data = api_get(
            url,
            {
                "vs_currency": LIST_CURRENCY,
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "sparkline": False,
            },
        )
        if data:
            coins.extend(data)
        time.sleep(CG_DELAY)

    return coins[:limit]


# ══════════════════════════════════════════════════════
# Binance (1단계)
# ══════════════════════════════════════════════════════


def binance_symbol(symbol: str) -> str:
    return f"{symbol.upper()}{BINANCE_QUOTE}"


def binance_fetch_all_klines(symbol: str) -> list[list]:
    """전체 기간 일봉 수집 (1000개씩 페이징)"""
    url = "https://api.binance.com/api/v3/klines"
    all_klines, start_time = [], 0

    while True:
        data = api_get(
            url,
            {
                "symbol": symbol,
                "interval": "1d",
                "startTime": start_time,
                "limit": 1000,
            },
        )
        time.sleep(BINANCE_DELAY)

        if not data:
            break

        all_klines.extend(data)

        if len(data) < 1000:
            break

        start_time = data[-1][6] + 1  # close_time + 1ms

    return all_klines


def parse_binance_klines(klines: list[list]) -> list[dict]:
    """
    kline index:
      [0] open_time  [1] open   [2] high  [3] low   [4] close
      [5] volume(base)  [6] close_time  [7] volume(quote=BTC)  [8] trade_count
    """
    result = []
    for k in klines:
        result.append(
            {
                "date": ts_to_date(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume_base": float(k[5]),
                "volume_quote": float(k[7]),
                "trade_count": int(k[8]),
                "source": "binance",
            }
        )
    return result


# ══════════════════════════════════════════════════════
# CryptoCompare (2단계 - Binance 상장 이전 보완)
# ══════════════════════════════════════════════════════


def cc_fetch_before(symbol: str, before_date: str) -> list[dict]:
    """
    before_date 이전의 전체 일별 OHLCV 수집
    - 한 번에 최대 2000개, toTs로 페이징
    - before_date 하루 전까지만 가져옴
    """
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    to_ts = date_to_ts(before_date) - 86400  # before_date 하루 전
    all_data = []

    while True:
        data = api_get(
            url,
            {
                "fsym": symbol.upper(),
                "tsym": CC_QUOTE,
                "limit": 2000,
                "toTs": to_ts,
                "api_key": CC_API_KEY,
            },
        )
        time.sleep(CC_DELAY)

        if not data or data.get("Response") != "Success":
            log.warning(
                f"  CC 응답 실패: {data.get('Message') if data else 'No response'}"
            )
            break

        rows = data["Data"]["Data"]

        # 가격이 모두 0인 행 제거 (상장 이전 빈 데이터)
        rows = [r for r in rows if r["close"] != 0]

        if not rows:
            break

        all_data = rows + all_data  # 오래된 순으로 앞에 붙임

        # 첫 번째 행의 time이 가장 오래된 것 → 더 이전으로 페이징
        earliest_ts = rows[0]["time"]
        to_ts = earliest_ts - 86400  # 하루 전으로 이동

        if len(rows) < 2000:
            break  # 더 이상 데이터 없음

    # dict 변환
    result = []
    for r in all_data:
        result.append(
            {
                "date": datetime.fromtimestamp(r["time"], tz=timezone.utc).strftime(
                    "%Y-%m-%d"
                ),
                "open": r["open"],
                "high": r["high"],
                "low": r["low"],
                "close": r["close"],
                "volume_base": r["volumefrom"],  # base asset 거래량
                "volume_quote": r["volumeto"],  # quote(BTC) 거래량
                "trade_count": 0,  # CC는 거래 횟수 미제공
                "source": "cryptocompare",
            }
        )
    return result


# ══════════════════════════════════════════════════════
# DB 저장
# ══════════════════════════════════════════════════════


def save_coin(conn: sqlite3.Connection, coin: dict):
    conn.execute(
        """
        INSERT OR REPLACE INTO coins (id, symbol, name, rank, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """,
        (
            coin["id"],
            coin["symbol"],
            coin["name"],
            coin.get("market_cap_rank"),
            datetime.now(tz=timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def save_rows(conn: sqlite3.Connection, coin_id: str, rows: list[dict]) -> int:
    data = [
        (
            coin_id,
            r["date"],
            r["open"],
            r["high"],
            r["low"],
            r["close"],
            r["volume_base"],
            r["volume_quote"],
            r["trade_count"],
            r["source"],
        )
        for r in rows
    ]

    conn.executemany(
        """
        INSERT OR IGNORE INTO ohlcv
            (coin_id, date, open, high, low, close,
             volume_base, volume_quote, trade_count, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        data,
    )
    conn.commit()
    return len(data)


# ══════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════


def main():
    log.info("=" * 55)
    log.info("암호화폐 데이터 수집 시작")
    log.info("  1단계: Binance OHLCV")
    log.info("  2단계: CryptoCompare (Binance 이전 데이터 보완)")
    log.info("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # 코인 목록 (CoinGecko)
    log.info("상위 100개 코인 목록 조회 중...")
    coins = cg_fetch_top_coins(limit=100)
    coins = [c for c in coins if c["id"] != "bitcoin"]  # BTC 제외
    log.info(f"BTC 제외 → {len(coins)}개 알트코인")

    for i, coin in enumerate(coins, 1):
        coin_id = coin["id"]
        symbol = coin["symbol"].upper()
        bn_symbol = binance_symbol(symbol)

        log.info(f"\n[{i}/{len(coins)}] {symbol} ({coin_id})")
        save_coin(conn, coin)

        # ── 1단계: Binance ──────────────────────────────
        log.info(f"  [1단계] Binance {bn_symbol} 수집 중...")
        klines = binance_fetch_all_klines(bn_symbol)

        if klines:
            rows = parse_binance_klines(klines)
            saved = save_rows(conn, coin_id, rows)
            earliest = rows[0]["date"]
            log.info(f"  Binance: {saved}일치 저장 (최초: {earliest})")
        else:
            log.warning(f"  Binance {bn_symbol} 없음 → 2단계에서 전체 수집")
            earliest = None

        # ── 2단계: CryptoCompare (이전 데이터 보완) ─────
        # Binance 데이터가 있으면 그 이전만, 없으면 전체 수집
        if earliest:
            log.info(f"  [2단계] CC {symbol}/BTC → {earliest} 이전 데이터 수집 중...")
            cc_rows = cc_fetch_before(symbol, before_date=earliest)
        else:
            log.info(f"  [2단계] CC {symbol}/BTC → 전체 데이터 수집 중...")
            # 오늘 날짜 기준으로 전체 수집
            today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            cc_rows = cc_fetch_before(symbol, before_date=today)

        if cc_rows:
            saved_cc = save_rows(conn, coin_id, cc_rows)
            log.info(f"  CC 보완: {saved_cc}일치 저장 (최초: {cc_rows[0]['date']})")
        else:
            log.info(f"  CC 보완 데이터 없음")

    conn.close()
    log.info("\n" + "=" * 55)
    log.info(f"수집 완료! DB: {DB_PATH}")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
