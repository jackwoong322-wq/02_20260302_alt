"""
알트코인 4년 주기 사이클 분석기 (USDT 페어)

- 데이터 소스: lib.common.config.DB_PATH (pairUSDT/crypto_usdt.db, ohlcv 테이블, USDT 페어)
- 결과 저장:
    alt_cycle_data    : 일별 OHLCV (기존)
    alt_cycle_summary : 코인별 사이클 Peak/Low 요약 (신규)

Peak 확정 로직:
  1. 전체 데이터 시작점부터 날짜 순으로 순회
  2. 각 날짜에 대해 Peak 조건 체크:
     ① 이후 1년 동안 고점 갱신 없음
     ② 이후 3년 안에 50% 이상 하락한 시점 존재
  3. 조건 만족하는 첫 날짜 = Peak 확정
  4. 다음 탐색은 Peak 이후 3년 뒤부터
  5. 반복

alt_cycle_summary 구조:
  peak_date / peak_price
  peak_pct_from_low  : 직전 저점 대비 +%  (from prev low)
  low_date  / low_price : 이 Peak ~ 다음 Peak 사이 최저점
  low_pct_from_peak  : 직전 고점 대비 -%  (from prev peak)
  prev_peak_date / prev_peak_price
  prev_low_date  / prev_low_price
"""

import sqlite3
import pandas as pd
from datetime import datetime, timezone

from lib.common.config import DB_PATH


def make_cycle_name(peak_ts: int, is_current: bool = False) -> str:
    """Peak timestamp(ms)로부터 사이클 이름 생성 (예: Cycle 2017, Current Cycle 2025)"""
    year = datetime.fromtimestamp(peak_ts / 1000, tz=timezone.utc).year
    if is_current:
        return f"Current Cycle ({year})"
    return f"Cycle {year}"


ONE_DAY_MS = 86_400_000
ONE_YEAR_MS = int(365.25 * ONE_DAY_MS)
PEAK_CONFIRM_MS = int(365 * 1 * ONE_DAY_MS)  # 1년 동안 갱신 없어야 Peak 확정
NEXT_SEARCH_MS = 2 * ONE_YEAR_MS  # Peak 후 다음 탐색 시작: 2년 뒤
PEAK_DRAWDOWN_RATE = 0.50  # 고점 대비 50% 이상 하락해야 확정


# ══════════════════════════════════════════════════════
# DB 초기화
# ══════════════════════════════════════════════════════


def init_cycle_table(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS alt_cycle_data (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id         TEXT    NOT NULL,
            cycle_number    INTEGER NOT NULL,
            cycle_name      TEXT,
            days_since_peak INTEGER NOT NULL,
            timestamp       TEXT    NOT NULL,
            close_price     REAL,
            low_price       REAL,
            high_price      REAL,
            close_rate      REAL,
            low_rate        REAL,
            high_rate       REAL,
            peak_date       TEXT,
            peak_price      REAL,
            UNIQUE(coin_id, cycle_number, days_since_peak)
        );

        CREATE TABLE IF NOT EXISTS alt_cycle_summary (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id             TEXT    NOT NULL,
            cycle_number        INTEGER NOT NULL,
            cycle_name          TEXT,

            -- Peak 정보
            peak_date           TEXT,
            peak_price          REAL,
            peak_pct_from_low   REAL,   -- 직전 저점 대비 +% (from prev low)

            -- Low 정보 (이 Peak ~ 다음 Peak 사이)
            low_date            TEXT,
            low_price           REAL,
            low_pct_from_peak   REAL,   -- 직전 고점 대비 -% (from prev peak)

            -- 직전 Peak/Low 참조
            prev_peak_date      TEXT,
            prev_peak_price     REAL,
            prev_low_date       TEXT,
            prev_low_price      REAL,

            UNIQUE(coin_id, cycle_number)
        );

        CREATE INDEX IF NOT EXISTS idx_alt_cycle_coin
            ON alt_cycle_data(coin_id);
        CREATE INDEX IF NOT EXISTS idx_alt_cycle_coin_cycle
            ON alt_cycle_data(coin_id, cycle_number);
        CREATE INDEX IF NOT EXISTS idx_alt_summary_coin
            ON alt_cycle_summary(coin_id);
    """
    )
    conn.commit()


# ══════════════════════════════════════════════════════
# 유틸
# ══════════════════════════════════════════════════════


def date_to_ms(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y/%m/%d")


# ══════════════════════════════════════════════════════
# OHLCV 로드
# ══════════════════════════════════════════════════════


def load_ohlcv(conn: sqlite3.Connection, coin_id: str) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT date, high, low, close
        FROM ohlcv
        WHERE coin_id = ?
        ORDER BY date ASC
    """,
        conn,
        params=(coin_id,),
    )

    if df.empty:
        return df

    df["timestamp"] = df["date"].apply(date_to_ms)
    df["close"] = df["close"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)

    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════
# Peak 탐지
# ══════════════════════════════════════════════════════


def is_confirmed_peak(df: pd.DataFrame, pos: int) -> bool:
    peak_ts = df.iloc[pos]["timestamp"]
    peak_high = df.iloc[pos]["high"]

    after_df = df[df["timestamp"] > peak_ts]
    if after_df.empty:
        return False

    confirm_end_ts = peak_ts + PEAK_CONFIRM_MS
    within_1yr = after_df[after_df["timestamp"] <= confirm_end_ts]

    if within_1yr.empty:
        return False

    # ① 1년 내 고점 갱신하면 가짜 Peak
    if within_1yr["high"].max() >= peak_high:
        return False

    # ② 3년 안에 50% 이상 하락
    within_3yr = after_df[after_df["timestamp"] <= peak_ts + 3 * ONE_YEAR_MS]
    drawdown_threshold = peak_high * (1 - PEAK_DRAWDOWN_RATE)
    if within_3yr["low"].min() > drawdown_threshold:
        return False

    return True


def find_all_peaks(df: pd.DataFrame, symbol: str = "") -> list[tuple]:
    """
    확정된 Peak 목록을 반환.
    symbol 인자는 디버그 로그 출력용.
    """
    if df.empty or len(df) < 365:
        return []

    peaks = []
    start_ts = df["timestamp"].min()
    end_ts = df["timestamp"].max()

    while start_ts < end_ts:
        search_df = df[df["timestamp"] >= start_ts]
        if search_df.empty:
            break

        peak_found = False
        for pos in search_df.index:
            if is_confirmed_peak(df, pos):
                peak_ts = int(df.iloc[pos]["timestamp"])
                peak_high = float(df.iloc[pos]["high"])
                peaks.append((peak_ts, peak_high))

                cycle_num = len(peaks)
                print(
                    f"    [Peak {cycle_num} 확정] {ms_to_date(peak_ts)}"
                    f"  @ {peak_high:>14,.4f}"
                    f"  (다음 탐색: {ms_to_date(peak_ts + NEXT_SEARCH_MS)}~)"
                )

                start_ts = peak_ts + NEXT_SEARCH_MS
                peak_found = True
                break

        if not peak_found:
            break

    return peaks


# ══════════════════════════════════════════════════════
# Low 탐지 (Peak ~ 다음Peak 구간 최저점)
# ══════════════════════════════════════════════════════


def find_low_between(df: pd.DataFrame, from_ts: int, to_ts: int = None) -> tuple:
    """
    from_ts ~ to_ts 구간에서 최저점 날짜/가격 반환
    to_ts=None 이면 끝까지
    """
    mask = df["timestamp"] > from_ts
    if to_ts:
        mask &= df["timestamp"] < to_ts
    seg = df[mask]
    if seg.empty:
        return None, None
    idx = seg["low"].idxmin()
    low_ts = seg.loc[idx, "timestamp"]
    low_price = seg.loc[idx, "low"]
    return int(low_ts), float(low_price)


# ══════════════════════════════════════════════════════
# 사이클 데이터 계산
# ══════════════════════════════════════════════════════


def calculate_cycle(
    df: pd.DataFrame,
    peak_ts: int,
    peak_high: float,
    cycle_num: int,
    next_peak_ts: int = None,
    is_current: bool = False,
) -> list[dict]:
    mask = df["timestamp"] >= peak_ts
    if next_peak_ts:
        mask &= df["timestamp"] < next_peak_ts

    cycle_df = df[mask].copy().reset_index(drop=True)
    peak_date = ms_to_date(peak_ts)
    cycle_name = make_cycle_name(peak_ts, is_current=is_current)
    records = []

    for i, row in cycle_df.iterrows():
        records.append(
            {
                "cycle_number": cycle_num,
                "cycle_name": cycle_name,
                "days_since_peak": i,
                "timestamp": ms_to_date(int(row["timestamp"])),
                "close_price": row["close"],
                "low_price": row["low"],
                "high_price": row["high"],
                "close_rate": (row["close"] / peak_high) * 100,
                "low_rate": (row["low"] / peak_high) * 100,
                "high_rate": (row["high"] / peak_high) * 100,
                "peak_date": peak_date,
                "peak_price": peak_high,
            }
        )

    return records


# ══════════════════════════════════════════════════════
# Summary 계산
# ══════════════════════════════════════════════════════


def build_summary(df: pd.DataFrame, peaks: list[tuple]) -> list[dict]:
    """
    peaks: [(peak_ts, peak_high), ...]
    각 사이클별 Peak/Low 요약 생성
    """
    summaries = []

    for idx, (peak_ts, peak_high) in enumerate(peaks):
        cycle_num = idx + 1
        is_last = idx == len(peaks) - 1
        next_peak_ts = peaks[idx + 1][0] if idx + 1 < len(peaks) else None
        is_current = is_last and (next_peak_ts is None)
        cycle_name = make_cycle_name(peak_ts, is_current=is_current)

        # ── Low: 이 Peak ~ 다음 Peak 사이 최저점 ──
        low_ts, low_price = find_low_between(df, peak_ts, next_peak_ts)

        # ── 직전 Peak ──
        if idx > 0:
            prev_peak_ts, prev_peak_price = peaks[idx - 1]
            prev_peak_date = ms_to_date(prev_peak_ts)
        else:
            prev_peak_ts = None
            prev_peak_price = None
            prev_peak_date = None

        # ── 직전 Low: 이전 Peak ~ 이 Peak 사이 최저점 ──
        prev_from_ts = prev_peak_ts if prev_peak_ts else df["timestamp"].min() - 1
        prev_low_ts, prev_low_price = find_low_between(df, prev_from_ts, peak_ts)

        # ── % 계산 ──
        if prev_low_price and prev_low_price > 0:
            peak_pct_from_low = ((peak_high - prev_low_price) / prev_low_price) * 100
        else:
            peak_pct_from_low = None

        if low_price and peak_high > 0:
            low_pct_from_peak = ((low_price - peak_high) / peak_high) * 100
        else:
            low_pct_from_peak = None

        summaries.append(
            {
                "cycle_number": cycle_num,
                "cycle_name": cycle_name,
                "peak_date": ms_to_date(peak_ts),
                "peak_price": peak_high,
                "peak_pct_from_low": (
                    round(peak_pct_from_low, 2)
                    if peak_pct_from_low is not None
                    else None
                ),
                "low_date": ms_to_date(low_ts) if low_ts else None,
                "low_price": low_price,
                "low_pct_from_peak": (
                    round(low_pct_from_peak, 2)
                    if low_pct_from_peak is not None
                    else None
                ),
                "prev_peak_date": prev_peak_date,
                "prev_peak_price": prev_peak_price,
                "prev_low_date": ms_to_date(prev_low_ts) if prev_low_ts else None,
                "prev_low_price": prev_low_price,
            }
        )

    return summaries


# ══════════════════════════════════════════════════════
# DB 저장
# ══════════════════════════════════════════════════════


def save_cycle_data(conn: sqlite3.Connection, coin_id: str, records: list[dict]) -> int:
    conn.execute("DELETE FROM alt_cycle_data WHERE coin_id = ?", (coin_id,))
    if not records:
        conn.commit()
        return 0

    conn.executemany(
        """
        INSERT OR REPLACE INTO alt_cycle_data
            (coin_id, cycle_number, cycle_name, days_since_peak, timestamp,
             close_price, low_price, high_price,
             close_rate, low_rate, high_rate,
             peak_date, peak_price)
        VALUES
            (:coin_id, :cycle_number, :cycle_name, :days_since_peak, :timestamp,
             :close_price, :low_price, :high_price,
             :close_rate, :low_rate, :high_rate,
             :peak_date, :peak_price)
    """,
        [dict(r, coin_id=coin_id) for r in records],
    )

    conn.commit()
    return len(records)


def save_summary(conn: sqlite3.Connection, coin_id: str, summaries: list[dict]) -> int:
    conn.execute("DELETE FROM alt_cycle_summary WHERE coin_id = ?", (coin_id,))
    if not summaries:
        conn.commit()
        return 0

    conn.executemany(
        """
        INSERT OR REPLACE INTO alt_cycle_summary
            (coin_id, cycle_number, cycle_name,
             peak_date, peak_price, peak_pct_from_low,
             low_date, low_price, low_pct_from_peak,
             prev_peak_date, prev_peak_price,
             prev_low_date, prev_low_price)
        VALUES
            (:coin_id, :cycle_number, :cycle_name,
             :peak_date, :peak_price, :peak_pct_from_low,
             :low_date, :low_price, :low_pct_from_peak,
             :prev_peak_date, :prev_peak_price,
             :prev_low_date, :prev_low_price)
    """,
        [dict(s, coin_id=coin_id) for s in summaries],
    )

    conn.commit()
    return len(summaries)


# ══════════════════════════════════════════════════════
# 로그 출력 (Peak/Low 상세)
# ══════════════════════════════════════════════════════


def date_diff_days(date_from: str, date_to: str) -> int:
    """YYYY/MM/DD 형식 두 날짜 간 일수 차이"""
    fmt = "%Y/%m/%d"
    d1 = datetime.strptime(date_from, fmt)
    d2 = datetime.strptime(date_to, fmt)
    return abs((d2 - d1).days)


def print_coin_result(summaries: list[dict]):
    for s in summaries:
        # Peak 라인
        if s["peak_pct_from_low"] is not None and s["prev_low_date"]:
            days = date_diff_days(s["prev_low_date"], s["peak_date"])
            peak_str = (
                f"  Peak : {s['peak_date']} @ {s['peak_price']:>14,.4f} USDT"
                f"  (+{s['peak_pct_from_low']:.1f}% from prev low in {days}d)"
            )
        else:
            peak_str = f"  Peak : {s['peak_date']} @ {s['peak_price']:>14,.4f} USDT"
        print(peak_str)

        # Low 라인
        if s["low_date"]:
            if s["low_pct_from_peak"] is not None:
                days = date_diff_days(s["peak_date"], s["low_date"])
                low_str = (
                    f"   Low : {s['low_date']} @ {s['low_price']:>14,.4f} USDT"
                    f"  ({s['low_pct_from_peak']:.1f}% from prev peak in {days}d)"
                )
            else:
                low_str = f"   Low : {s['low_date']} @ {s['low_price']:>14,.4f} USDT"
            print(low_str)


# ══════════════════════════════════════════════════════
# 요약 출력
# ══════════════════════════════════════════════════════


def print_summary(conn: sqlite3.Connection):
    rows = conn.execute(
        """
        SELECT coin_id,
               COUNT(DISTINCT cycle_number) as cycles,
               MIN(timestamp)               as earliest,
               MAX(timestamp)               as latest,
               COUNT(*)                     as total_rows
        FROM alt_cycle_data
        GROUP BY coin_id
        ORDER BY coin_id
    """
    ).fetchall()

    print(f"\n{'코인':<20} {'사이클':>6} {'시작':>12} {'끝':>12} {'총행수':>8}")
    print("-" * 65)
    for r in rows:
        print(f"{r[0]:<20} {r[1]:>6} {r[2]:>12} {r[3]:>12} {r[4]:>8}")


# ══════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════


def main():
    print("=" * 60)
    print("알트코인 사이클 분석 시작 (USDT 페어)")
    print(f"  Peak 조건 ①: 이후 1년 동안 고점 갱신 없음")
    print(f"  Peak 조건 ②: 고점 대비 {int(PEAK_DRAWDOWN_RATE*100)}% 이상 하락")
    print(f"  다음 탐색  : Peak 이후 3년 뒤부터")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    init_cycle_table(conn)

    coins = conn.execute("SELECT id, symbol FROM coins ORDER BY rank").fetchall()

    if not coins:
        print(
            "[ERROR] coins 테이블 비어있음. crypto_collector_usdt.py 먼저 실행하세요."
        )
        conn.close()
        return

    print(f"총 {len(coins)}개 코인 분석 시작\n")

    success, skipped, no_peak = 0, 0, 0

    for i, (coin_id, symbol) in enumerate(coins, 1):
        print(f"[{i}/{len(coins)}] {symbol} ({coin_id})")

        df = load_ohlcv(conn, coin_id)
        if df.empty or len(df) < 365:
            print(f"  → 데이터 부족 ({len(df)}일), 건너뜀\n")
            skipped += 1
            continue

        print(f"  데이터: {len(df)}일 ({df['date'].iloc[0]} ~ {df['date'].iloc[-1]})")

        # ── Peak 탐지 (디버그 로그 포함) ────────────────
        peaks = find_all_peaks(df, symbol)

        if not peaks:
            print(f"  → Peak 없음, 건너뜀\n")
            no_peak += 1
            continue

        # ── Current Cycle 탐지 ──────────────────────────
        last_peak_ts, last_peak_high = peaks[-1]
        last_cycle_num = len(peaks)
        current_search_ts = last_peak_ts + NEXT_SEARCH_MS

        today_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        after_3yr = df[
            (df["timestamp"] >= current_search_ts) & (df["timestamp"] <= today_ts)
        ]
        if not after_3yr.empty:
            current_cycle_num = last_cycle_num + 1

            current_peak_idx = after_3yr["high"].idxmax()
            current_peak_ts = int(df.loc[current_peak_idx, "timestamp"])
            current_peak_high = float(df.loc[current_peak_idx, "high"])

            print(
                f"    [Current Peak]  {ms_to_date(current_peak_ts)}"
                f"  @ {current_peak_high:>14,.4f}"
            )

            peaks.append((current_peak_ts, current_peak_high))

        # ── 사이클 데이터 계산 ──────────────────────────
        all_records = []
        for idx, (peak_ts, peak_high) in enumerate(peaks):
            cycle_num = idx + 1
            next_peak_ts = peaks[idx + 1][0] if idx + 1 < len(peaks) else None
            is_current = (next_peak_ts is None) and (idx == len(peaks) - 1)
            records = calculate_cycle(
                df, peak_ts, peak_high, cycle_num, next_peak_ts, is_current
            )
            all_records.extend(records)

        # ── Summary 계산 ────────────────────────────────
        summaries = build_summary(df, peaks)

        # ── 로그 출력 ────────────────────────────────────
        print_coin_result(summaries)
        print(f"  → {len(peaks)}개 사이클, {len(all_records)}행 저장 ✓\n")

        # ── DB 저장 ──────────────────────────────────────
        save_cycle_data(conn, coin_id, all_records)
        save_summary(conn, coin_id, summaries)
        success += 1

    print("=" * 60)
    print(f"완료: 성공 {success}개 / Peak없음 {no_peak}개 / 데이터부족 {skipped}개")
    print("=" * 60)

    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
