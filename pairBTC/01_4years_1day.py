import os
import pandas as pd
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv
from pathlib import Path

# --- 환경 변수 로드 ---
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # .env 파일의 키 이름에 맞춤

# --- 설정 ---
OHLCV_TABLE_NAME = "ohlcv_1day"
CYCLE_TABLE_NAME = "bitcoin_cycle_data"

ONE_DAY_MS = 86400000
SEVEN_DAYS_MS = 7 * ONE_DAY_MS
THREE_YEARS_MS = int(3 * 365.25 * 24 * 60 * 60 * 1000)
FIVE_YEARS_MS = int(5 * 365.25 * 24 * 60 * 60 * 1000)

CYCLE_NAMES = {1: "2013 Cycle", 2: "2017 Cycle", 3: "2021 Cycle", 4: "2025 Cycle"}

# 알려진 비트코인 사이클 Peak 날짜 (UTC)
KNOWN_PEAK_DATES = {
    1: "2013/12/04",  # $1,237
    2: "2017/12/15",  # $19,783
    3: "2021/11/08",  # $67,525
    # 4: 자동 탐지
}


def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL과 SUPABASE_KEY 환경변수를 설정하세요")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def ms_to_date(timestamp_ms):
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime(
        "%Y/%m/%d"
    )


def date_to_ms(date_str):
    dt = datetime.strptime(date_str, "%Y/%m/%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def normalize_timestamp(ts):
    return ts * 1000 if ts < 10000000000 else ts


def get_last_saved_info(supabase: Client):
    """Cycle 4의 마지막 저장된 timestamp 조회"""
    response = (
        supabase.table(CYCLE_TABLE_NAME)
        .select("timestamp")
        .eq("cycle_number", 4)
        .order("days_since_peak", desc=True)
        .limit(1)
        .execute()
    )

    if response.data and len(response.data) > 0:
        timestamp_str = response.data[0]["timestamp"]
        try:
            return date_to_ms(timestamp_str), timestamp_str
        except:
            pass
    return None, None


def get_cycle4_peak_info(supabase: Client):
    """Cycle 4의 Peak 정보 조회"""
    response = (
        supabase.table(CYCLE_TABLE_NAME)
        .select("timestamp, close_price")
        .eq("cycle_number", 4)
        .eq("days_since_peak", 0)
        .execute()
    )

    if response.data and len(response.data) > 0:
        row = response.data[0]
        try:
            return date_to_ms(row["timestamp"]), row["close_price"]
        except:
            pass
    return None, None


def get_ohlcv_data(supabase: Client, from_timestamp_ms=None):
    """OHLCV 데이터 조회"""
    query = supabase.table(OHLCV_TABLE_NAME).select("timestamp, close, low, high")

    if from_timestamp_ms:
        query = query.gte("timestamp", from_timestamp_ms)

    # Supabase는 기본 1000개 제한이 있으므로 페이지네이션 필요
    all_data = []
    offset = 0
    batch_size = 1000

    while True:
        response = (
            query.order("timestamp", desc=False)
            .range(offset, offset + batch_size - 1)
            .execute()
        )

        if not response.data:
            break

        all_data.extend(response.data)

        if len(response.data) < batch_size:
            break

        offset += batch_size

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df["timestamp"] = df["timestamp"].apply(normalize_timestamp)
    return df


def find_peak(df, start_ts, end_ts=None):
    """특정 기간 내 Peak 찾기"""
    mask = df["timestamp"] >= start_ts
    if end_ts:
        mask &= df["timestamp"] <= end_ts
    period_df = df[mask]

    if period_df.empty:
        return None, None

    max_close = period_df["close"].astype(float).max()
    peak_row = period_df[period_df["close"] == max_close].iloc[0]
    return peak_row["timestamp"], peak_row["close"]


def calculate_cycle_data(df, peak_ts, peak_close, cycle_num, end_ts=None):
    """사이클 데이터 계산"""
    mask = df["timestamp"] >= peak_ts
    if end_ts:
        mask &= df["timestamp"] <= end_ts
    cycle_df = df[mask].copy()

    if cycle_df.empty:
        return None

    cycle_df[f"{cycle_num}_timestamp"] = cycle_df["timestamp"].apply(ms_to_date)
    cycle_df[f"{cycle_num}_close"] = cycle_df["close"]
    cycle_df[f"{cycle_num}_low"] = cycle_df["low"]
    cycle_df[f"{cycle_num}_high"] = cycle_df["high"]
    cycle_df[f"{cycle_num}_rate"] = (cycle_df["close"] / peak_close) * 100
    cycle_df[f"{cycle_num}_low_rate"] = (cycle_df["low"] / peak_close) * 100
    cycle_df[f"{cycle_num}_high_rate"] = (cycle_df["high"] / peak_close) * 100

    cycle_df = cycle_df.reset_index(drop=True)
    cycle_df.index.name = "day_index"

    return cycle_df[
        [
            f"{cycle_num}_timestamp",
            f"{cycle_num}_close",
            f"{cycle_num}_low",
            f"{cycle_num}_high",
            f"{cycle_num}_rate",
            f"{cycle_num}_low_rate",
            f"{cycle_num}_high_rate",
        ]
    ]


def find_all_peaks(df):
    """모든 사이클의 Peak 찾기 (알려진 날짜 사용)"""
    peaks = []

    # Cycle 1-3: 알려진 Peak 날짜 사용
    for cycle_num in [1, 2, 3]:
        if cycle_num in KNOWN_PEAK_DATES:
            peak_date = KNOWN_PEAK_DATES[cycle_num]
            peak_ts = date_to_ms(peak_date)
            # 해당 날짜의 close 가격 찾기
            peak_row = df[df["timestamp"] == peak_ts]
            if not peak_row.empty:
                peak_close = float(peak_row["close"].iloc[0])
                peaks.append((peak_ts, peak_close))
                print(
                    f"[INFO] Cycle {cycle_num} Peak: {peak_date} @ ${peak_close:,.2f}"
                )
            else:
                print(f"[WARN] Cycle {cycle_num} Peak 날짜({peak_date})에 데이터 없음")

    # Cycle 4: 2021 Peak 이후 3년 뒤부터 자동 탐지
    if len(peaks) >= 3:
        cycle3_peak_ts = peaks[2][0]
        search_start = cycle3_peak_ts + THREE_YEARS_MS
        max_ts = df["timestamp"].max()

        if search_start < max_ts:
            peak_ts, peak_close = find_peak(df, search_start, max_ts)
            if peak_ts:
                peaks.append((peak_ts, peak_close))
                print(
                    f"[INFO] Cycle 4 Peak: {ms_to_date(peak_ts)} @ ${peak_close:,.2f}"
                )

    return peaks


def convert_to_long_format(result_df):
    """Wide format -> Long format 변환"""
    long_data = []

    for _, row in result_df.iterrows():
        days_since_peak = row["Days_Since_Peak"]

        for cycle_num in [1, 2, 3, 4]:
            ts_col = f"{cycle_num}_timestamp"
            close_col = f"{cycle_num}_close"
            if (
                ts_col in row.index
                and close_col in row.index
                and pd.notna(row[ts_col])
                and pd.notna(row[close_col])
                and str(row[ts_col]).strip()
            ):
                long_data.append(
                    {
                        "cycle_number": cycle_num,
                        "cycle_name": CYCLE_NAMES[cycle_num],
                        "days_since_peak": days_since_peak,
                        "timestamp": str(row[ts_col]).strip(),
                        "close_price": row.get(f"{cycle_num}_close"),
                        "low_price": row.get(f"{cycle_num}_low"),
                        "high_price": row.get(f"{cycle_num}_high"),
                        "close_rate": row.get(f"{cycle_num}_rate"),
                        "low_rate": row.get(f"{cycle_num}_low_rate"),
                        "high_rate": row.get(f"{cycle_num}_high_rate"),
                    }
                )

    return pd.DataFrame(long_data)


def save_full_data(supabase: Client, result_df):
    """전체 데이터 저장 (기존 데이터 삭제 후 저장)"""
    long_df = convert_to_long_format(result_df)
    if long_df.empty:
        return

    # 기존 데이터 삭제
    for cycle_num in [1, 2, 3, 4]:
        supabase.table(CYCLE_TABLE_NAME).delete().eq(
            "cycle_number", cycle_num
        ).execute()

    # 배치로 저장 (Supabase는 한 번에 너무 많은 데이터 삽입 시 문제 발생 가능)
    batch_size = 500
    records = long_df.to_dict("records")

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        supabase.table(CYCLE_TABLE_NAME).upsert(batch).execute()

    print(f"총 {len(long_df)}개 레코드 저장")
    for cycle_num in [1, 2, 3, 4]:
        count = len(long_df[long_df["cycle_number"] == cycle_num])
        if count > 0:
            print(f"  - Cycle {cycle_num}: {count}개")


def save_incremental_data(supabase: Client, df):
    """증분 데이터 저장"""
    if df is None or df.empty:
        return 0

    records = df.to_dict("records")
    supabase.table(CYCLE_TABLE_NAME).upsert(records).execute()

    return len(records)


def run_full_analysis(supabase: Client):
    """전체 분석 실행"""
    print("\n=== 전체 분석 모드 ===")

    df = get_ohlcv_data(supabase)
    if df.empty:
        print("데이터 없음")
        return False

    print(f"총 {len(df)}개 데이터 로드")

    peaks = find_all_peaks(df)
    if not peaks:
        print("[ERROR] Peak 없음")
        return False

    final_df = None
    for i, (peak_ts, peak_close) in enumerate(peaks):
        cycle_num = i + 1
        end_ts = peaks[i + 1][0] - ONE_DAY_MS if i < len(peaks) - 1 else None
        cycle_df = calculate_cycle_data(df, peak_ts, peak_close, cycle_num, end_ts)

        if final_df is None:
            final_df = cycle_df
        else:
            final_df = final_df.merge(
                cycle_df, left_index=True, right_index=True, how="outer"
            )

    # 인덱스를 Days_Since_Peak 컬럼으로 변환
    final_df = final_df.reset_index(drop=False)
    if "index" in final_df.columns:
        final_df = final_df.rename(columns={"index": "Days_Since_Peak"})
    else:
        final_df["Days_Since_Peak"] = range(len(final_df))

    save_full_data(supabase, final_df)
    return True


def run_incremental_update(supabase: Client, last_timestamp_ms):
    """증분 업데이트 실행"""
    print("\n=== 증분 업데이트 모드 ===")
    print(f"마지막 저장: {ms_to_date(last_timestamp_ms)}")

    peak_ts, peak_close = get_cycle4_peak_info(supabase)
    if peak_ts is None:
        print("[WARN] Cycle 4 Peak 없음 → 전체 분석")
        return run_full_analysis(supabase)

    print(f"Cycle 4 Peak: {ms_to_date(peak_ts)} @ ${peak_close:,.2f}")

    from_ts = last_timestamp_ms - SEVEN_DAYS_MS
    from_date = ms_to_date(from_ts)
    print(f"재계산 시작: {from_date}")

    # 기존 데이터 삭제 (해당 기간)
    supabase.table(CYCLE_TABLE_NAME).delete().eq("cycle_number", 4).gte(
        "timestamp", from_date
    ).execute()

    df = get_ohlcv_data(supabase, from_ts)
    print(f"조회 데이터: {len(df)}개")

    if df.empty:
        return True

    result_data = []
    for _, row in df.iterrows():
        ts = row["timestamp"]
        days_since_peak = int((ts - peak_ts) / ONE_DAY_MS)
        if days_since_peak < 0:
            continue

        result_data.append(
            {
                "cycle_number": 4,
                "cycle_name": CYCLE_NAMES[4],
                "days_since_peak": days_since_peak,
                "timestamp": ms_to_date(ts),
                "close_price": float(row["close"]),
                "low_price": float(row["low"]),
                "high_price": float(row["high"]),
                "close_rate": (float(row["close"]) / peak_close) * 100,
                "low_rate": (float(row["low"]) / peak_close) * 100,
                "high_rate": (float(row["high"]) / peak_close) * 100,
            }
        )

    saved = save_incremental_data(supabase, pd.DataFrame(result_data))
    print(f"저장: {saved}개")
    return True


def print_summary(supabase: Client):
    """사이클별 요약 출력"""
    print("\n사이클별 요약:")

    for cycle_num in [1, 2, 3, 4]:
        # 카운트
        count_response = (
            supabase.table(CYCLE_TABLE_NAME)
            .select("*", count="exact")
            .eq("cycle_number", cycle_num)
            .execute()
        )

        # 최소/최대 timestamp
        min_response = (
            supabase.table(CYCLE_TABLE_NAME)
            .select("timestamp")
            .eq("cycle_number", cycle_num)
            .order("days_since_peak", desc=False)
            .limit(1)
            .execute()
        )

        max_response = (
            supabase.table(CYCLE_TABLE_NAME)
            .select("timestamp")
            .eq("cycle_number", cycle_num)
            .order("days_since_peak", desc=True)
            .limit(1)
            .execute()
        )

        count = count_response.count or 0
        if count > 0:
            min_ts = min_response.data[0]["timestamp"] if min_response.data else "N/A"
            max_ts = max_response.data[0]["timestamp"] if max_response.data else "N/A"
            print(f"  Cycle {cycle_num}: {count}개 ({min_ts} ~ {max_ts})")


def main():
    print("=== Bitcoin 4년 주기 분석 (Supabase) ===\n")

    try:
        supabase = get_supabase_client()
        print("Supabase 연결 성공")
    except Exception as e:
        print(f"[ERROR] Supabase 연결 실패: {e}")
        return

    try:
        last_ts, _ = get_last_saved_info(supabase)

        if last_ts is None:
            run_full_analysis(supabase)
        else:
            run_incremental_update(supabase, last_ts)

        print_summary(supabase)
        print("\n완료")

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
