import sqlite3

from lib.common.config import DB_PATH


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("DISTINCT scenario from coin_prediction_paths:")
    cur.execute(
        "SELECT DISTINCT scenario FROM coin_prediction_paths ORDER BY scenario"
    )
    for row in cur.fetchall():
        print(row)

    for sym in ("BTC", "ETH", "BNB", "XRP"):
        print(f"\n{sym} counts:")
        for scenario in ("bear", "bull"):
            cur.execute(
                """
                SELECT COUNT(*) FROM coin_prediction_paths
                WHERE symbol=? AND scenario=?
                """,
                (sym, scenario),
            )
            cnt = cur.fetchone()[0]
            print(f"  {scenario}: {cnt}")

    conn.close()


if __name__ == "__main__":
    main()

