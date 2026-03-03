# pairUSDT/lib/visualizer/db.py
import sqlite3


def load_all_coins(conn: sqlite3.Connection) -> list:
    """실측(alt_cycle_data) 또는 박스 분석(coin_analysis_results)이 있는 코인 포함.
    예측 유무와 관계없이 실측만 있어도 차트 대상에 넣기 위함."""
    return conn.execute(
        """
        SELECT c.id, c.symbol, c.name, c.rank
        FROM coins c
        WHERE EXISTS (SELECT 1 FROM alt_cycle_data a WHERE a.coin_id = c.id)
           OR EXISTS (SELECT 1 FROM coin_analysis_results r WHERE r.coin_id = c.id)
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


def load_box_zones(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        """
        SELECT coin_id, cycle_number, box_index,
               phase, result,
               start_x, end_x,
               hi, lo, hi_day, lo_day,
               duration, range_pct,
               is_prediction, is_completed
        FROM coin_analysis_results
        ORDER BY coin_id, cycle_number, box_index
        """
    ).fetchall()

    result: dict = {}
    for row in rows:
        coin_id, cycle_num = row[0], row[1]
        if coin_id not in result:
            result[coin_id] = {}
        if cycle_num not in result[coin_id]:
            result[coin_id][cycle_num] = []

        rp = row[12]
        result[coin_id][cycle_num].append(
            {
                "boxIndex": row[2],
                "startX": row[5],
                "endX": row[6],
                "hi": row[7],
                "lo": row[8],
                "hiDay": row[9],
                "loDay": row[10],
                "duration": row[11],
                "rangePct": f"{rp:.1f}" if rp is not None else "0.0",
                "phase": row[3],
                "result": row[4],
                "is_prediction": row[13],
                "is_completed": row[14],
            }
        )

    return result


def load_prediction_paths(conn: sqlite3.Connection) -> dict:
    try:
        rows = conn.execute(
            """
            SELECT coin_id, cycle_number, scenario, day_x, value
            FROM coin_prediction_paths
            ORDER BY coin_id, cycle_number, scenario, day_x
            """
        ).fetchall()
    except Exception:
        return {}

    result: dict = {}
    for coin_id, cycle_num, scenario, day_x, value in rows:
        if coin_id not in result:
            result[coin_id] = {}
        if cycle_num not in result[coin_id]:
            result[coin_id][cycle_num] = {"bull": [], "bear": []}
        key = scenario.lower() if scenario and scenario.lower() in ("bull", "bear") else "bull"
        result[coin_id][cycle_num][key].append({"x": day_x, "value": value})
    return result


def load_peak_predictions(conn: sqlite3.Connection) -> dict:
    try:
        rows = conn.execute(
            """
            SELECT coin_id, cycle_number, peak_type, predicted_value, predicted_day
            FROM coin_prediction_peaks
            ORDER BY coin_id, cycle_number
            """
        ).fetchall()
    except Exception:
        return {}

    result: dict = {}
    for coin_id, cycle_num, peak_type, value, day_x in rows:
        if coin_id not in result:
            result[coin_id] = {}
        if cycle_num not in result[coin_id]:
            result[coin_id][cycle_num] = []
        result[coin_id][cycle_num].append({
            "type": peak_type,
            "value": value,
            "day_x": day_x,
        })
    return result


def build_json(conn: sqlite3.Connection, coins: list) -> dict:
    box_zones = load_box_zones(conn)
    pred_paths = load_prediction_paths(conn)
    peak_preds = load_peak_predictions(conn)

    total_bz = sum(len(zones) for coin_zones in box_zones.values() for zones in coin_zones.values())
    print(
        f"[DB] coin_analysis_results 로드: {total_bz}개 박스 "
        f"({'DB 데이터 사용' if total_bz > 0 else '없음 → JS 폴백'})"
    )

    result: dict = {}
    for coin_id, symbol, name, rank in coins:
        cycles = load_cycle_data(conn, coin_id)
        if not cycles:
            # 실측 가격(alt_cycle_data)은 없지만 박스(coin_analysis_results)만 있는 코인:
            # 박스 기준으로 최소 사이클 구성 → 실측만 있어도 차트에 표시
            coin_zones = box_zones.get(coin_id, {})
            if not coin_zones:
                continue
            for cn, zlist in coin_zones.items():
                cycles[cn] = {
                    "cycle_number": cn,
                    "cycle_name": f"Cycle {cn}",
                    "peak_date": "",
                    "peak_price": None,
                    "data": [],
                }

        cycles_list = []
        for cn, cycle_data in sorted(cycles.items()):
            cycle_zones = box_zones.get(coin_id, {}).get(cn, [])
            cycle_paths = pred_paths.get(coin_id, {}).get(cn, {"bull": [], "bear": []})
            cycle_peaks = peak_preds.get(coin_id, {}).get(cn, [])
            cycles_list.append(
                {
                    "cycle_number": cn,
                    "cycle_name": cycle_data["cycle_name"],
                    "peak_date": cycle_data["peak_date"],
                    "peak_price": cycle_data["peak_price"],
                    "data": cycle_data["data"],
                    "box_zones": cycle_zones,
                    "prediction_paths": cycle_paths,
                    "peak_predictions": cycle_peaks,
                }
            )

        result[coin_id] = {
            "symbol": symbol.upper(),
            "name": name,
            "rank": rank,
            "cycles": cycles_list,
        }

    return result