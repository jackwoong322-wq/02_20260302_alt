"""
알짜배기 코인 선별 스코어링

지표 및 가중치:
  1. 상승률        (40%) - 사이클마다 BTC 대비 고점이 얼마나 높았는지
  2. 하락 저항성   (20%) - 하락 구간에서 얼마나 버텼는지 (저점이 얕을수록 좋음)
  3. 사이클 반복성 (20%) - 사이클별 패턴이 얼마나 일정한지
  4. 회복 속도     (20%) - 저점 → 다음 고점 회복까지 걸린 일수

조건: 확정 사이클(Current Cycle 제외) 2개 이상인 코인만 평가
출력: HTML 랭킹 테이블
"""

import sqlite3
import json
import webbrowser
import numpy as np
from pathlib import Path

DB_PATH = "crypto_data.db"
OUT_FILE = "coin_ranking.html"


# ══════════════════════════════════════════════════════
# 데이터 로드
# ══════════════════════════════════════════════════════


def load_coins(conn):
    return conn.execute(
        """
        SELECT id, symbol, name, rank
        FROM coins ORDER BY rank
    """
    ).fetchall()


def load_cycles(conn, coin_id):
    """확정 사이클만 로드 (Current Cycle 제외)"""
    rows = conn.execute(
        """
        SELECT cycle_number, cycle_name, days_since_peak,
               close_rate, high_rate, low_rate, peak_price
        FROM alt_cycle_data
        WHERE coin_id = ? AND cycle_name != 'Current Cycle'
        ORDER BY cycle_number, days_since_peak
    """,
        (coin_id,),
    ).fetchall()

    cycles = {}
    for row in rows:
        cn = row[0]
        if cn not in cycles:
            cycles[cn] = {
                "cycle_number": cn,
                "cycle_name": row[1],
                "peak_price": row[6],
                "data": [],
            }
        cycles[cn]["data"].append(
            {
                "day": row[2],
                "close": row[3],
                "high": row[4],
                "low": row[5],
            }
        )
    return list(cycles.values())


# ══════════════════════════════════════════════════════
# 지표 계산
# ══════════════════════════════════════════════════════


def calc_gain_score(cycles):
    """
    상승률 점수 (40%)
    각 사이클의 고점(peak_price)이 이전 사이클 대비 얼마나 올랐는지
    → 사이클간 peak_price 성장률 평균
    → peak_price가 없으면 close_rate 최대값(=100%) 기준 대신
       close_rate 기준 최고점 사용
    """
    if len(cycles) < 2:
        return 0.0

    # 각 사이클의 최고 close_rate (= 100에 가까울수록 peak에 가까움)
    # peak_price 절대값보다 사이클 내 최고 high_rate를 씀
    peaks = []
    for c in cycles:
        max_high = max(d["high"] for d in c["data"])
        peaks.append(max_high)

    # 사이클마다 이전 대비 성장 여부 → 평균 성장률
    growths = []
    for i in range(1, len(peaks)):
        if peaks[i - 1] > 0:
            growths.append(peaks[i] / peaks[i - 1])

    if not growths:
        return 0.0

    avg_growth = np.mean(growths)
    # 1.0 이상이면 성장, 이하면 하락
    # 점수화: 0~100 사이로 정규화 (growth 1.0 = 50점, 2.0 = 100점, 0.5 = 0점)
    score = min(100, max(0, (avg_growth - 0.5) / 1.5 * 100))
    return round(score, 2)


def calc_drawdown_score(cycles):
    """
    하락 저항성 점수 (20%)
    사이클별 최저점(close_rate 최솟값) 평균
    → 저점이 높을수록(덜 빠질수록) 좋음
    → 저점 평균이 높을수록 고점수
    """
    bottoms = []
    for c in cycles:
        min_close = min(d["close"] for d in c["data"])
        bottoms.append(min_close)

    avg_bottom = np.mean(bottoms)
    # 저점 0% = 0점, 50% = 100점
    score = min(100, max(0, avg_bottom * 2))
    return round(score, 2)


def calc_consistency_score(cycles):
    """
    사이클 반복성 점수 (20%)
    사이클별 close_rate 패턴의 유사도
    → 같은 day 기준으로 사이클간 표준편차가 작을수록 패턴이 일정
    → 공통 구간(모든 사이클이 가진 day 범위)만 비교
    """
    if len(cycles) < 2:
        return 0.0

    # 공통 최소 길이
    min_len = min(len(c["data"]) for c in cycles)
    if min_len < 30:
        return 0.0

    # 각 사이클의 close_rate 배열 (앞 min_len개)
    arrays = []
    for c in cycles:
        arr = [d["close"] for d in c["data"][:min_len]]
        arrays.append(arr)

    # day별 표준편차 평균
    stds = []
    for i in range(min_len):
        vals = [arr[i] for arr in arrays]
        stds.append(np.std(vals))

    avg_std = np.mean(stds)
    # std 0 = 완벽한 일치(100점), std 50 = 0점
    score = min(100, max(0, (1 - avg_std / 50) * 100))
    return round(score, 2)


def calc_recovery_score(cycles):
    """
    회복 속도 점수 (20%)
    저점 이후 peak(100%) 수준까지 회복하는 데 걸린 일수
    → 빠를수록 고점수
    → 회복 못하면 0점
    """
    recovery_days_list = []

    for c in cycles:
        data = c["data"]
        if not data:
            continue

        # 저점 위치
        min_close = min(d["close"] for d in data)
        bottom_idx = next(i for i, d in enumerate(data) if d["close"] == min_close)

        # 저점 이후 80% 이상 회복한 첫 날
        recovered = False
        for i in range(bottom_idx, len(data)):
            if data[i]["close"] >= 80:
                recovery_days_list.append(i - bottom_idx)
                recovered = True
                break

        if not recovered:
            recovery_days_list.append(len(data))  # 회복 못함 = 최대 패널티

    if not recovery_days_list:
        return 0.0

    avg_days = np.mean(recovery_days_list)
    # 회복 0일 = 100점, 365일 이상 = 0점
    score = min(100, max(0, (1 - avg_days / 365) * 100))
    return round(score, 2)


# ══════════════════════════════════════════════════════
# 종합 점수
# ══════════════════════════════════════════════════════

WEIGHTS = {
    "gain": 0.40,
    "drawdown": 0.20,
    "consistency": 0.20,
    "recovery": 0.20,
}


def calc_total_score(scores):
    return round(
        scores["gain"] * WEIGHTS["gain"]
        + scores["drawdown"] * WEIGHTS["drawdown"]
        + scores["consistency"] * WEIGHTS["consistency"]
        + scores["recovery"] * WEIGHTS["recovery"],
        2,
    )


# ══════════════════════════════════════════════════════
# 전체 코인 스코어링
# ══════════════════════════════════════════════════════


def score_all_coins(conn):
    coins = load_coins(conn)
    results = []

    for coin_id, symbol, name, rank in coins:
        cycles = load_cycles(conn, coin_id)

        # 확정 사이클 2개 미만 제외
        if len(cycles) < 2:
            continue

        scores = {
            "gain": calc_gain_score(cycles),
            "drawdown": calc_drawdown_score(cycles),
            "consistency": calc_consistency_score(cycles),
            "recovery": calc_recovery_score(cycles),
        }
        scores["total"] = calc_total_score(scores)
        scores["cycles"] = len(cycles)

        # 사이클별 저점 정보
        bottoms = []
        for c in cycles:
            min_close = min(d["close"] for d in c["data"])
            min_day = next(d["day"] for d in c["data"] if d["close"] == min_close)
            bottoms.append(
                {"cycle": c["cycle_number"], "rate": min_close, "day": min_day}
            )

        results.append(
            {
                "coin_id": coin_id,
                "symbol": symbol.upper(),
                "name": name,
                "rank": rank or 999,
                "scores": scores,
                "bottoms": bottoms,
            }
        )

    # 총점 내림차순 정렬
    results.sort(key=lambda x: x["scores"]["total"], reverse=True)

    # 랭킹 부여
    for i, r in enumerate(results):
        r["score_rank"] = i + 1

    return results


# ══════════════════════════════════════════════════════
# HTML 생성
# ══════════════════════════════════════════════════════

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Altcoin Ranking</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@700;800&display=swap');
  :root {
    --bg:      #080c14;
    --panel:   #0d1420;
    --border:  #1e2d45;
    --accent:  #00d4ff;
    --text:    #c8d8f0;
    --dim:     #4a6080;
    --green:   #00ff88;
    --red:     #ff4466;
    --gold:    #FFB800;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    min-height: 100vh;
  }

  .header {
    padding: 20px 32px;
    border-bottom: 1px solid var(--border);
    background: var(--panel);
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .header-title {
    font-family: 'Syne', sans-serif;
    font-size: 20px;
    font-weight: 800;
    color: white;
    letter-spacing: 2px;
  }
  .header-title span { color: var(--accent); }
  .header-sub { font-size: 11px; color: var(--dim); letter-spacing: 1px; margin-top: 4px; }

  .toolbar {
    padding: 12px 32px;
    border-bottom: 1px solid var(--border);
    display: flex;
    gap: 12px;
    align-items: center;
    flex-wrap: wrap;
    background: var(--panel);
  }
  .toolbar input {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 7px 12px;
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    outline: none;
    width: 200px;
  }
  .toolbar input:focus { border-color: var(--accent); }
  .toolbar select {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 7px 12px;
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    outline: none;
    cursor: pointer;
  }
  .toolbar label { font-size: 10px; color: var(--dim); letter-spacing: 1px; }
  .count-badge {
    margin-left: auto;
    font-size: 11px;
    color: var(--dim);
  }
  .count-badge span { color: var(--accent); font-weight: 600; }

  .table-wrap {
    padding: 24px 32px;
    overflow-x: auto;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }
  thead th {
    padding: 10px 14px;
    text-align: left;
    font-size: 10px;
    letter-spacing: 1.5px;
    color: var(--dim);
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
    cursor: pointer;
    user-select: none;
    transition: color 0.15s;
  }
  thead th:hover { color: var(--accent); }
  thead th.sorted { color: var(--accent); }
  thead th .sort-arrow { margin-left: 4px; opacity: 0.5; }
  thead th.sorted .sort-arrow { opacity: 1; }

  tbody tr {
    border-bottom: 1px solid rgba(30,45,69,0.5);
    transition: background 0.12s;
  }
  tbody tr:hover { background: rgba(0,212,255,0.04); }

  td { padding: 11px 14px; vertical-align: middle; }

  .rank-col { color: var(--dim); font-size: 11px; width: 40px; }
  .rank-top { color: var(--gold); font-weight: 600; }

  .symbol-col { font-weight: 600; color: white; font-size: 13px; }
  .name-col   { color: var(--dim); font-size: 11px; }
  .mktrank-col { color: var(--dim); font-size: 11px; }
  .cycles-col { color: var(--dim); text-align: center; }

  .score-total {
    font-size: 15px;
    font-weight: 600;
    font-family: 'Syne', sans-serif;
  }

  .score-bar-wrap {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 120px;
  }
  .score-bar {
    flex: 1;
    height: 4px;
    background: var(--border);
    border-radius: 2px;
    overflow: hidden;
  }
  .score-bar-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 0.3s;
  }
  .score-val { font-size: 11px; width: 34px; text-align: right; }

  .sub-scores {
    display: flex;
    gap: 6px;
  }
  .sub-score {
    display: flex;
    flex-direction: column;
    align-items: center;
    min-width: 50px;
  }
  .sub-score-label { font-size: 9px; color: var(--dim); letter-spacing: 0.5px; margin-bottom: 3px; }
  .sub-score-val   { font-size: 11px; }

  .bottom-list { font-size: 10px; color: var(--dim); }
  .bottom-item { white-space: nowrap; }
  .bottom-item span { color: var(--red); }

  .medal { font-size: 14px; }

  /* Score color gradient */
  .s-high { color: #00ff88; }
  .s-mid  { color: #FFB800; }
  .s-low  { color: #ff6644; }
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="header-title">ALT<span>/</span>BTC <span style="font-weight:400;font-size:14px;color:var(--dim)">COIN RANKING</span></div>
    <div class="header-sub">CYCLE QUALITY SCORE · 2+ CONFIRMED CYCLES · BTC PAIR</div>
  </div>
  <div style="text-align:right">
    <div style="font-size:11px;color:var(--dim)">WEIGHTS</div>
    <div style="font-size:10px;color:var(--text);margin-top:4px">
      GAIN 40% &nbsp;·&nbsp; DRAWDOWN 20% &nbsp;·&nbsp; CONSISTENCY 20% &nbsp;·&nbsp; RECOVERY 20%
    </div>
  </div>
</div>

<div class="toolbar">
  <label>SEARCH</label>
  <input type="text" id="searchInput" placeholder="Symbol or name...">
  <label>MIN CYCLES</label>
  <select id="cycleFilter">
    <option value="2">2+</option>
    <option value="3">3+</option>
  </select>
  <label>SORT BY</label>
  <select id="sortSelect" onchange="sortBy(this.value)">
    <option value="total">Total Score</option>
    <option value="gain">Gain</option>
    <option value="drawdown">Drawdown</option>
    <option value="consistency">Consistency</option>
    <option value="recovery">Recovery</option>
    <option value="rank">Market Rank</option>
  </select>
  <div class="count-badge">Showing <span id="showCount">0</span> coins</div>
</div>

<div class="table-wrap">
<table id="rankTable">
  <thead>
    <tr>
      <th>#</th>
      <th>SYMBOL</th>
      <th>NAME</th>
      <th>MKT</th>
      <th>CYC</th>
      <th onclick="sortBy('total')" class="sorted">TOTAL SCORE <span class="sort-arrow">↓</span></th>
      <th>GAIN <span style="font-size:9px;color:#4a6080">(40%)</span></th>
      <th>DRAWDOWN <span style="font-size:9px;color:#4a6080">(20%)</span></th>
      <th>CONSISTENCY <span style="font-size:9px;color:#4a6080">(20%)</span></th>
      <th>RECOVERY <span style="font-size:9px;color:#4a6080">(20%)</span></th>
      <th>CYCLE BOTTOMS</th>
    </tr>
  </thead>
  <tbody id="tableBody"></tbody>
</table>
</div>

<script>
const DATA = __RANKING_DATA__;
let currentSort = 'total';
let sortAsc = false;

function scoreColor(v) {
  if (v >= 65) return 's-high';
  if (v >= 35) return 's-mid';
  return 's-low';
}

function scoreBarColor(v) {
  if (v >= 65) return '#00ff88';
  if (v >= 35) return '#FFB800';
  return '#ff6644';
}

function medal(rank) {
  if (rank === 1) return '<span class="medal">🥇</span>';
  if (rank === 2) return '<span class="medal">🥈</span>';
  if (rank === 3) return '<span class="medal">🥉</span>';
  return rank;
}

function renderTable(data) {
  const tbody = document.getElementById('tableBody');
  tbody.innerHTML = '';
  document.getElementById('showCount').textContent = data.length;

  data.forEach((d, i) => {
    const s  = d.scores;
    const tr = document.createElement('tr');
    const rankClass = d.score_rank <= 3 ? 'rank-top' : 'rank-col';

    const bottomsHtml = d.bottoms.map(b =>
      `<div class="bottom-item">C${b.cycle}: <span>${b.rate.toFixed(1)}%</span> (d${b.day})</div>`
    ).join('');

    const scoreBar = (val, label) => `
      <div class="score-bar-wrap">
        <div class="score-bar">
          <div class="score-bar-fill" style="width:${val}%;background:${scoreBarColor(val)}"></div>
        </div>
        <div class="score-val ${scoreColor(val)}">${val.toFixed(0)}</div>
      </div>`;

    tr.innerHTML = `
      <td class="${rankClass}">${medal(d.score_rank)}</td>
      <td class="symbol-col">${d.symbol}</td>
      <td class="name-col">${d.name}</td>
      <td class="mktrank-col">#${d.rank}</td>
      <td class="cycles-col">${s.cycles}</td>
      <td>
        <div style="display:flex;align-items:center;gap:10px">
          <div class="score-total ${scoreColor(s.total)}">${s.total.toFixed(1)}</div>
          <div class="score-bar" style="width:80px">
            <div class="score-bar-fill" style="width:${s.total}%;background:${scoreBarColor(s.total)}"></div>
          </div>
        </div>
      </td>
      <td>${scoreBar(s.gain,        'GAIN')}</td>
      <td>${scoreBar(s.drawdown,    'DD')}</td>
      <td>${scoreBar(s.consistency, 'CON')}</td>
      <td>${scoreBar(s.recovery,    'REC')}</td>
      <td><div class="bottom-list">${bottomsHtml}</div></td>
    `;
    tbody.appendChild(tr);
  });
}

function getFiltered() {
  const search   = document.getElementById('searchInput').value.toLowerCase();
  const minCycle = parseInt(document.getElementById('cycleFilter').value);
  return DATA.filter(d =>
    (d.symbol.toLowerCase().includes(search) || d.name.toLowerCase().includes(search)) &&
    d.scores.cycles >= minCycle
  );
}

function sortBy(key) {
  if (currentSort === key) sortAsc = !sortAsc;
  else { currentSort = key; sortAsc = false; }

  document.getElementById('sortSelect').value = key;

  const filtered = getFiltered();
  filtered.sort((a, b) => {
    let va = key === 'rank' ? a.rank : a.scores[key];
    let vb = key === 'rank' ? b.rank : b.scores[key];
    return sortAsc ? va - vb : vb - va;
  });
  renderTable(filtered);
}

document.getElementById('searchInput').addEventListener('input', () => sortBy(currentSort));
document.getElementById('cycleFilter').addEventListener('change', () => sortBy(currentSort));

// Init
sortBy('total');
</script>
</body>
</html>
"""


def main():
    conn = sqlite3.connect(DB_PATH)
    results = score_all_coins(conn)
    conn.close()

    if not results:
        print("[ERROR] No scoring data. Run alt_cycle_analysis.py first.")
        return

    print(f"\n{'='*55}")
    print(f"Scoring complete: {len(results)} coins evaluated")
    print(f"{'='*55}")
    print(
        f"{'RANK':<5} {'SYMBOL':<8} {'TOTAL':>6} {'GAIN':>6} {'DD':>6} {'CON':>6} {'REC':>6}"
    )
    print("-" * 55)
    for r in results[:20]:
        s = r["scores"]
        print(
            f"{r['score_rank']:<5} {r['symbol']:<8} "
            f"{s['total']:>6.1f} {s['gain']:>6.1f} "
            f"{s['drawdown']:>6.1f} {s['consistency']:>6.1f} {s['recovery']:>6.1f}"
        )

    html = HTML.replace("__RANKING_DATA__", json.dumps(results, ensure_ascii=False))
    out = Path(OUT_FILE)
    out.write_text(html, encoding="utf-8")
    print(f"\nHTML saved: {out.resolve()}")
    webbrowser.open(f"file://{out.resolve()}")


if __name__ == "__main__":
    main()
