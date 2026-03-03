"""
알트코인 4년 주기 사이클 분석기

- 데이터 소스: crypto_data.db (ohlcv 테이블, BTC 페어)
- 결과 저장: alt_cycle_data 테이블 (동일 DB)

Peak 확정 로직:
  1. 전체 데이터 시작점부터 날짜 순으로 순회
  2. 각 날짜에 대해 Peak 조건 체크:
     ① 이후 365*3일(3년) 동안 고점 갱신 없음
     ② 이후 전체 구간에서 70% 이상 하락한 시점 존재
  3. 조건 만족하는 첫 날짜 = Peak 확정
  4. 다음 탐색은 Peak 이후 3년 뒤부터
  5. 반복
"""

import sqlite3
import pandas as pd
from datetime import datetime, timezone

# ── 설정 ──────────────────────────────────────────────
DB_PATH = "crypto_data.db"
CYCLE_NAMES = {1: "Cycle 1", 2: "Cycle 2", 3: "Cycle 3", 4: "Cycle 4", 5: "Cycle 5"}

ONE_DAY_MS = 86_400_000
ONE_YEAR_MS = int(365.25 * ONE_DAY_MS)
PEAK_CONFIRM_MS = int(365 * 3 * ONE_DAY_MS)  # 4년 동안 갱신 없어야 Peak 확정
NEXT_SEARCH_MS = 3 * ONE_YEAR_MS  # Peak 후 다음 탐색 시작: 4년 뒤
PEAK_DRAWDOWN_RATE = 0.10  # 고점 대비 30% 이상 하락해야 확정


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
            timestamp       TEXT    NOT NULL,  -- YYYY/MM/DD
            close_price     REAL,
            low_price       REAL,
            high_price      REAL,
            close_rate      REAL,              -- Peak 대비 %
            low_rate        REAL,
            high_rate       REAL,
            peak_date       TEXT,
            peak_price      REAL,
            UNIQUE(coin_id, cycle_number, days_since_peak)
        );

        CREATE INDEX IF NOT EXISTS idx_alt_cycle_coin
            ON alt_cycle_data(coin_id);
        CREATE INDEX IF NOT EXISTS idx_alt_cycle_coin_cycle
            ON alt_cycle_data(coin_id, cycle_number);
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
    """
    Peak 확정 조건 (둘 다 만족해야):
      ① 이후 3년 동안 고점 갱신 없음
      ② 이후 3년 안에 40% 이상 하락한 시점 존재
    """
    peak_ts = df.iloc[pos]["timestamp"]
    peak_close = df.iloc[pos]["close"]

    after_df = df[df["timestamp"] > peak_ts]
    if after_df.empty:
        return False

    # 이후 3년 구간
    confirm_end_ts = peak_ts + PEAK_CONFIRM_MS
    within_3yr = after_df[after_df["timestamp"] <= confirm_end_ts]

    if within_3yr.empty:
        return False  # 3년치 데이터가 없으면 확정 불가

    # ① 3년 내 고점 갱신하면 가짜 Peak
    if within_3yr["close"].max() >= peak_close:
        return False

    # ② 3년 안에 40% 이상 하락한 시점 존재
    drawdown_threshold = peak_close * (1 - PEAK_DRAWDOWN_RATE)
    if within_3yr["close"].min() > drawdown_threshold:
        return False

    return True


def find_all_peaks(df: pd.DataFrame) -> list[tuple]:
    """
    전체 데이터를 순회하며 Peak 탐지
    - 시작점부터 날짜 순 순회
    - Peak 확정되면 3년 뒤부터 다음 탐색
    """
    if df.empty or len(df) < 365:
        return []

    peaks = []
    start_ts = df["timestamp"].min()
    end_ts = df["timestamp"].max()

    while start_ts < end_ts:
        # 탐색 범위 내 데이터
        search_df = df[df["timestamp"] >= start_ts]
        if search_df.empty:
            break

        peak_found = False

        # 날짜 순 순회 → 조건 만족하는 첫 번째 날이 Peak
        for pos in search_df.index:
            if is_confirmed_peak(df, pos):
                peak_ts = df.iloc[pos]["timestamp"]
                peak_close = df.iloc[pos]["close"]
                peaks.append((peak_ts, peak_close))

                # 다음 탐색은 Peak 이후 3년 뒤부터
                start_ts = peak_ts + NEXT_SEARCH_MS
                peak_found = True
                break

        if not peak_found:
            break  # 더 이상 Peak 없음

    return peaks


# ══════════════════════════════════════════════════════
# 사이클 데이터 계산
# ══════════════════════════════════════════════════════


def calculate_cycle(
    df: pd.DataFrame,
    peak_ts: int,
    peak_close: float,
    cycle_num: int,
    next_peak_ts: int = None,
) -> list[dict]:
    mask = df["timestamp"] >= peak_ts
    if next_peak_ts:
        mask &= df["timestamp"] < next_peak_ts

    cycle_df = df[mask].copy().reset_index(drop=True)
    peak_date = ms_to_date(peak_ts)
    records = []

    for i, row in cycle_df.iterrows():
        records.append(
            {
                "cycle_number": cycle_num,
                "cycle_name": CYCLE_NAMES.get(cycle_num, f"Cycle {cycle_num}"),
                "days_since_peak": i,
                "timestamp": ms_to_date(row["timestamp"]),
                "close_price": row["close"],
                "low_price": row["low"],
                "high_price": row["high"],
                "close_rate": (row["close"] / peak_close) * 100,
                "low_rate": (row["low"] / peak_close) * 100,
                "high_rate": (row["high"] / peak_close) * 100,
                "peak_date": peak_date,
                "peak_price": peak_close,
            }
        )

    return records


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
    print("=" * 55)
    print("알트코인 사이클 분석 시작")
    print(f"  Peak 조건 ①: 이후 3년 동안 고점 갱신 없음")
    print(f"  Peak 조건 ②: 고점 대비 {int(PEAK_DRAWDOWN_RATE*100)}% 이상 하락")
    print(f"  다음 탐색: Peak 이후 3년 뒤부터")
    print("=" * 55)

    conn = sqlite3.connect(DB_PATH)
    init_cycle_table(conn)

    coins = conn.execute("SELECT id, symbol FROM coins ORDER BY rank").fetchall()

    if not coins:
        print("[ERROR] coins 테이블 비어있음. crypto_collector.py 먼저 실행하세요.")
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

        peaks = find_all_peaks(df)

        if not peaks:
            print(f"  → Peak 없음, 건너뜀\n")
            no_peak += 1
            continue

        for p_ts, p_close in peaks:
            print(f"  Peak: {ms_to_date(p_ts)} @ {p_close:.8f} BTC")

        all_records = []
        for idx, (peak_ts, peak_close) in enumerate(peaks):
            cycle_num = idx + 1
            next_peak_ts = peaks[idx + 1][0] if idx + 1 < len(peaks) else None
            records = calculate_cycle(df, peak_ts, peak_close, cycle_num, next_peak_ts)
            all_records.extend(records)

        # ── 현재 진행 중인 사이클 처리 ──────────────────
        # 마지막 Peak 이후 3년 뒤부터 데이터가 있으면 Current Cycle로 저장
        last_peak_ts, last_peak_close = peaks[-1]
        last_cycle_num = len(peaks)
        current_search_ts = last_peak_ts + NEXT_SEARCH_MS  # 3년 뒤부터 탐색

        after_3yr = df[df["timestamp"] >= current_search_ts]
        if not after_3yr.empty:
            current_cycle_num = last_cycle_num + 1
            CYCLE_NAMES[current_cycle_num] = "Current Cycle"

            # 3년 뒤부터 현재까지 최고값을 임시 Peak로
            current_peak_idx = after_3yr["close"].idxmax()
            current_peak_ts = df.loc[current_peak_idx, "timestamp"]
            current_peak_close = df.loc[current_peak_idx, "close"]

            print(
                f"  Current Peak (진행중): {ms_to_date(current_peak_ts)} "
                f"@ {current_peak_close:.8f} BTC"
            )

            # 마지막 확정 사이클은 current_peak_ts 직전까지만
            all_records = [
                r
                for r in all_records
                if not (
                    r["cycle_number"] == last_cycle_num
                    and r["timestamp"] >= ms_to_date(current_peak_ts)
                )
            ]

            current_records = calculate_cycle(
                df, current_peak_ts, current_peak_close, current_cycle_num
            )
            all_records.extend(current_records)

        saved = save_cycle_data(conn, coin_id, all_records)
        print(f"  → {len(peaks)}개 사이클, {saved}행 저장 ✓\n")
        success += 1

    print("=" * 55)
    print(f"완료: 성공 {success}개 / Peak없음 {no_peak}개 / 데이터부족 {skipped}개")
    print("=" * 55)

    print_summary(conn)
    conn.close()


if __name__ == "__main__":
    main()
