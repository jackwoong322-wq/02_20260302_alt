import sqlite3

from lib.common.config import DB_PATH
from lib.visualizer.db import build_json, load_all_coins


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    coins = load_all_coins(conn)
    data = build_json(conn, coins)
    conn.close()

    btc_id = None
    for cid, info in data.items():
        if (info.get("symbol") or "").upper() == "BTC":
            btc_id = cid
            break

    print("BTC coin_id in ALL_DATA:", btc_id)
    if btc_id is None:
        return

    btc = data[btc_id]
    print("BTC cycles count:", len(btc.get("cycles", [])))
    for cyc in btc.get("cycles", []):
        print(
            "cycle_number:",
            cyc["cycle_number"],
            "name:",
            cyc["cycle_name"],
            "data_len:",
            len(cyc.get("data", [])),
        )


if __name__ == "__main__":
    main()

