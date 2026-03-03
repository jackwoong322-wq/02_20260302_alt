import sqlite3

from lib.common.config import DB_PATH


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("=== coins table BTC row ===")
    cur.execute(
        "SELECT id, symbol, name, rank FROM coins WHERE UPPER(symbol)='BTC' LIMIT 5"
    )
    for row in cur.fetchall():
        print(row)

    print("\n=== coin_analysis_results BTC is_prediction=1 sample ===")
    cur.execute(
        """
        SELECT DISTINCT coin_id, symbol, cycle_number, is_prediction
        FROM coin_analysis_results
        WHERE UPPER(symbol)='BTC' AND is_prediction=1
        ORDER BY cycle_number
        LIMIT 10
        """
    )
    for row in cur.fetchall():
        print(row)

    print("\n=== coin_prediction_paths BTC sample ===")
    cur.execute(
        """
        SELECT DISTINCT coin_id, symbol, cycle_number, scenario
        FROM coin_prediction_paths
        WHERE UPPER(symbol)='BTC'
        ORDER BY cycle_number
        LIMIT 10
        """
    )
    for row in cur.fetchall():
        print(row)

    print("\n=== alt_cycle_data BTC cycles sample ===")
    cur.execute(
        """
        SELECT DISTINCT cycle_number
        FROM alt_cycle_data a
        JOIN coins c ON a.coin_id = c.id
        WHERE UPPER(c.symbol)='BTC'
        ORDER BY cycle_number
        """
    )
    for row in cur.fetchall():
        print(row)

    conn.close()


if __name__ == "__main__":
    main()

