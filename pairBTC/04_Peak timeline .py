"""
Peak 순환 타임라인 시각화
- CHAINLINK 기준 각 코인의 peak 선후행 관계
- 2021/2024 사이클 비교 + 다음 사이클 예측
"""

import pandas as pd
import numpy as np
import json
import webbrowser
from pathlib import Path

DB_CSV = "alt_cycle_export.csv"
OUT_FILE = "peak_timeline.html"


def prepare_data(csv_path):
    df = pd.read_csv(csv_path)
    confirmed = df[df["cycle_name"] != "Current Cycle"].copy()
    peaks = (
        confirmed.groupby(["coin_id", "cycle_number"])["peak_date"]
        .first()
        .reset_index()
    )
    peaks["peak_dt"] = pd.to_datetime(peaks["peak_date"], format="%Y/%m/%d")

    def era(dt):
        y = dt.year
        if y <= 2018:
            return "2017era"
        if y <= 2022:
            return "2021era"
        return "2024era"

    peaks["era"] = peaks["peak_dt"].apply(era)

    era21 = (
        peaks[peaks["era"] == "2021era"].sort_values("peak_dt").reset_index(drop=True)
    )
    era24 = (
        peaks[peaks["era"] == "2024era"].sort_values("peak_dt").reset_index(drop=True)
    )

    for e in [era21, era24]:
        base = e.iloc[0]["peak_dt"]
        e["days_from_first"] = (e["peak_dt"] - base).dt.days

    common = set(era21["coin_id"]) & set(era24["coin_id"])
    base_coin = "chainlink"
    base21 = int(era21[era21["coin_id"] == base_coin].iloc[0]["days_from_first"])
    base24 = int(era24[era24["coin_id"] == base_coin].iloc[0]["days_from_first"])
    cl_peak_24 = era24[era24["coin_id"] == base_coin].iloc[0]["peak_dt"]

    LABELS = {
        "tether": "TETHER",
        "chainlink": "CHAINLINK",
        "dogecoin": "DOGE",
        "ripple": "XRP",
        "hedera-hashgraph": "HBAR",
        "solana": "SOL",
        "shiba-inu": "SHIB",
        "avalanche-2": "AVAX",
        "ethereum": "ETH",
        "binancecoin": "BNB",
        "the-open-network": "TON",
        "usd-coin": "USDC",
    }

    timeline = []
    for coin in common:
        r21 = era21[era21["coin_id"] == coin].iloc[0]
        r24 = era24[era24["coin_id"] == coin].iloc[0]
        d21 = int(r21["days_from_first"]) - base21
        d24 = int(r24["days_from_first"]) - base24
        avg = round((d21 + d24) / 2)
        pred_dt = cl_peak_24 + pd.Timedelta(days=avg)
        timeline.append(
            {
                "coin": LABELS.get(coin, coin.upper()),
                "coin_id": coin,
                "d21": d21,
                "d24": d24,
                "avg": avg,
                "consistent": abs(d21 - d24) < 80,
                "peak_21": r21["peak_date"],
                "peak_24": r24["peak_date"],
                "pred_date": pred_dt.strftime("%Y-%m-%d"),
                "rank_21": int(r21.name) + 1,
                "rank_24": int(r24.name) + 1,
            }
        )

    timeline.sort(key=lambda x: x["avg"])
    return {
        "data": timeline,
        "cl_peak": cl_peak_24.strftime("%Y-%m-%d"),
        "cl_label": "CHAINLINK",
    }


HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Peak Rotation Timeline</title>
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
  --gold:    #FFB800;
  --red:     #ff4466;
  --c21:     #4fc3f7;
  --c24:     #FFB800;
  --cpred:   #ff6b9d;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { background:var(--bg); color:var(--text); font-family:'JetBrains Mono',monospace; min-height:100vh; }

.header {
  padding:20px 32px; border-bottom:1px solid var(--border);
  background:var(--panel); display:flex; justify-content:space-between; align-items:center;
}
.title { font-family:'Syne',sans-serif; font-size:20px; font-weight:800; color:white; letter-spacing:2px; }
.title span { color:var(--accent); }
.subtitle { font-size:11px; color:var(--dim); margin-top:4px; letter-spacing:1px; }

.legend-bar {
  display:flex; gap:24px; align-items:center;
  padding:12px 32px; border-bottom:1px solid var(--border); background:var(--panel);
  flex-wrap:wrap;
}
.leg { display:flex; align-items:center; gap:8px; font-size:11px; }
.leg-dot { width:10px; height:10px; border-radius:50%; }
.info-box {
  margin-left:auto; background:rgba(0,212,255,0.08); border:1px solid rgba(0,212,255,0.2);
  border-radius:6px; padding:8px 16px; font-size:11px; color:var(--accent);
}

/* ── Timeline ── */
.timeline-wrap { padding:32px; overflow-x:auto; }
.timeline-header {
  display:flex; align-items:center; gap:0;
  margin-bottom:8px; padding-left:120px;
  font-size:10px; color:var(--dim); letter-spacing:1px;
  position:relative;
}

.chart-area { position:relative; }

/* axis */
.axis-line {
  position:absolute; top:0; bottom:0;
  width:1px; background:var(--border); opacity:0.6;
  pointer-events:none;
}
.axis-label {
  position:absolute; top:-22px;
  font-size:9px; color:var(--dim);
  transform:translateX(-50%);
  white-space:nowrap;
}
.zero-line {
  position:absolute; top:0; bottom:0;
  width:2px; background:rgba(0,212,255,0.5);
  pointer-events:none;
}
.zero-label {
  position:absolute; top:-22px;
  font-size:9px; color:var(--accent); font-weight:600;
  transform:translateX(-50%);
}

/* coin rows */
.coin-row {
  display:flex; align-items:center;
  height:52px; border-bottom:1px solid rgba(30,45,69,0.4);
  position:relative;
  transition:background 0.15s;
}
.coin-row:hover { background:rgba(0,212,255,0.03); }

.coin-label {
  width:120px; min-width:120px;
  font-size:12px; font-weight:600; color:white;
  display:flex; flex-direction:column; gap:2px;
  padding-right:12px; text-align:right;
}
.coin-sub { font-size:9px; color:var(--dim); font-weight:400; }

.row-track { flex:1; position:relative; height:100%; }

/* dots */
.dot {
  position:absolute; top:50%; transform:translate(-50%,-50%);
  border-radius:50%; cursor:pointer;
  transition:all 0.2s;
  z-index:2;
}
.dot:hover { transform:translate(-50%,-50%) scale(1.4); z-index:10; }
.dot-21  { width:10px; height:10px; background:var(--c21); border:2px solid var(--c21); }
.dot-24  { width:12px; height:12px; background:var(--c24); border:2px solid var(--c24); }
.dot-pred { width:10px; height:10px; background:transparent; border:2px dashed var(--cpred); }
.dot-cl  { width:14px; height:14px; background:var(--accent); border:2px solid white; }

/* connector line between 21 and 24 */
.connector {
  position:absolute; top:50%; height:2px;
  transform:translateY(-50%);
  z-index:1; border-radius:1px;
}

/* tooltip */
.tooltip {
  position:fixed; background:rgba(8,12,20,0.95);
  border:1px solid var(--border); border-radius:6px;
  padding:10px 14px; font-size:11px; pointer-events:none;
  z-index:100; display:none; backdrop-filter:blur(8px);
  min-width:180px;
}
.tt-title { font-weight:600; color:white; margin-bottom:6px; font-size:12px; }
.tt-row   { display:flex; justify-content:space-between; gap:16px; margin:3px 0; }
.tt-label { color:var(--dim); }
.tt-val   { font-weight:600; }

/* table */
.table-section { padding:0 32px 40px; }
.section-title {
  font-family:'Syne',sans-serif; font-size:14px; font-weight:700;
  color:white; letter-spacing:2px; margin-bottom:16px;
  padding-bottom:8px; border-bottom:1px solid var(--border);
}
table { width:100%; border-collapse:collapse; font-size:11px; }
th {
  padding:8px 14px; text-align:left; font-size:10px; letter-spacing:1.5px;
  color:var(--dim); border-bottom:1px solid var(--border);
}
td { padding:10px 14px; border-bottom:1px solid rgba(30,45,69,0.4); }
tr:hover td { background:rgba(0,212,255,0.03); }
.consistent { color:var(--green); font-size:10px; }
.inconsistent { color:var(--dim); font-size:10px; }
.coin-name { font-weight:600; color:white; font-size:12px; }
.days-21 { color:var(--c21); }
.days-24 { color:var(--c24); }
.days-pred { color:var(--cpred); }
.pred-date { color:var(--cpred); font-weight:600; }
.past { color:var(--dim); text-decoration:line-through; }
.future { color:var(--green); }
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="title">PEAK <span>ROTATION</span> TIMELINE</div>
    <div class="subtitle">CHAINLINK 기준 알트코인 Peak 선후행 분석 · 2021 vs 2024 사이클</div>
  </div>
  <div style="text-align:right;font-size:11px;color:var(--dim)">
    <div>CHAINLINK PEAK (2024)</div>
    <div style="color:var(--accent);font-size:14px;font-weight:600;margin-top:4px" id="clPeakLabel"></div>
  </div>
</div>

<div class="legend-bar">
  <div class="leg"><div class="leg-dot" style="background:var(--c21)"></div> 2021 사이클 실제 peak</div>
  <div class="leg"><div class="leg-dot" style="background:var(--c24)"></div> 2024 사이클 실제 peak</div>
  <div class="leg"><div class="leg-dot" style="background:transparent;border:2px dashed var(--cpred);border-radius:50%;width:10px;height:10px"></div> 다음 사이클 예측</div>
  <div class="leg"><div class="leg-dot" style="background:var(--accent)"></div> CHAINLINK (기준점)</div>
  <div class="info-box">CHAINLINK peak 기준 상대 일수 (음수=선행, 양수=후행)</div>
</div>

<div class="timeline-wrap">
  <div class="timeline-header" id="axisHeader" style="height:24px;"></div>
  <div class="chart-area" id="chartArea"></div>
</div>

<div class="table-section">
  <div class="section-title">PEAK 선후행 상세 데이터</div>
  <table>
    <thead>
      <tr>
        <th>코인</th>
        <th>2021 peak</th>
        <th>2024 peak</th>
        <th style="color:var(--c21)">2021 지연(일)</th>
        <th style="color:var(--c24)">2024 지연(일)</th>
        <th>평균 지연</th>
        <th style="color:var(--cpred)">다음 사이클 예측</th>
        <th>일관성</th>
      </tr>
    </thead>
    <tbody id="tableBody"></tbody>
  </table>
</div>

<div class="tooltip" id="tooltip"></div>

<script>
const RAW = __DATA__;
const DATA = RAW.data;
const CL_PEAK = new Date(RAW.cl_peak);
const today = new Date();

document.getElementById('clPeakLabel').textContent = RAW.cl_peak;

// ── Timeline 렌더링 ──────────────────────────────────
const MIN_D = Math.min(...DATA.map(d => Math.min(d.d21, d.d24, d.avg))) - 50;
const MAX_D = Math.max(...DATA.map(d => Math.max(d.d21, d.d24, d.avg))) + 100;
const RANGE = MAX_D - MIN_D;
const TRACK_W = Math.max(900, window.innerWidth - 200);

function pct(d) { return ((d - MIN_D) / RANGE * 100).toFixed(3) + '%'; }
function dayToLeft(d) { return (d - MIN_D) / RANGE * TRACK_W; }

// axis
const axisHeader = document.getElementById('axisHeader');
const chartArea  = document.getElementById('chartArea');
axisHeader.style.position = 'relative';
axisHeader.style.marginLeft = '120px';
axisHeader.style.width = TRACK_W + 'px';

// grid lines
const ticks = [];
for (let d = Math.ceil(MIN_D/100)*100; d <= MAX_D; d += 100) ticks.push(d);
ticks.forEach(d => {
  const line = document.createElement('div');
  line.className = d === 0 ? 'zero-line' : 'axis-line';
  line.style.left = dayToLeft(d) + 'px';
  chartArea.appendChild(line);

  const lbl = document.createElement('div');
  lbl.className = d === 0 ? 'zero-label' : 'axis-label';
  lbl.textContent = d === 0 ? 'CHAINLINK' : (d > 0 ? `+${d}d` : `${d}d`);
  lbl.style.left = dayToLeft(d) + 'px';
  axisHeader.appendChild(lbl);
});

// coin rows
DATA.forEach(d => {
  const row = document.createElement('div');
  row.className = 'coin-row';

  const label = document.createElement('div');
  label.className = 'coin-label';
  label.innerHTML = `<span>${d.coin}</span><span class="coin-sub">#${d.rank_24} → #${d.rank_21}</span>`;
  row.appendChild(label);

  const track = document.createElement('div');
  track.className = 'row-track';
  track.style.width = TRACK_W + 'px';
  track.style.position = 'relative';

  // connector line
  const minD = Math.min(d.d21, d.d24);
  const maxD = Math.max(d.d21, d.d24);
  const conn = document.createElement('div');
  conn.className = 'connector';
  conn.style.left  = dayToLeft(minD) + 'px';
  conn.style.width = (dayToLeft(maxD) - dayToLeft(minD)) + 'px';
  conn.style.background = d.consistent
    ? 'rgba(0,212,255,0.15)' : 'rgba(255,100,100,0.1)';
  track.appendChild(conn);

  // 2021 dot
  makeDot(track, d.d21, 'dot-21', d, '2021');
  // 2024 dot
  makeDot(track, d.d24, d.coin_id === 'chainlink' ? 'dot-cl' : 'dot-24', d, '2024');
  // prediction dot (next cycle)
  if (d.coin_id !== 'chainlink') {
    makeDot(track, d.avg, 'dot-pred', d, 'pred');
  }

  row.appendChild(track);
  chartArea.appendChild(row);
});

function makeDot(parent, days, cls, d, type) {
  const dot = document.createElement('div');
  dot.className = 'dot ' + cls;
  dot.style.left = dayToLeft(days) + 'px';

  dot.addEventListener('mouseenter', e => showTooltip(e, d, type, days));
  dot.addEventListener('mousemove',  e => moveTooltip(e));
  dot.addEventListener('mouseleave', hideTooltip);
  parent.appendChild(dot);
}

// tooltip
const tt = document.getElementById('tooltip');
function showTooltip(e, d, type, days) {
  const predDate = new Date(CL_PEAK.getTime() + d.avg * 86400000);
  const isPast   = predDate < today;
  tt.innerHTML = `
    <div class="tt-title">${d.coin}</div>
    <div class="tt-row"><span class="tt-label">2021 peak</span><span class="tt-val" style="color:var(--c21)">${d.peak_21}</span></div>
    <div class="tt-row"><span class="tt-label">2024 peak</span><span class="tt-val" style="color:var(--c24)">${d.peak_24}</span></div>
    <div class="tt-row"><span class="tt-label">2021 지연</span><span class="tt-val" style="color:var(--c21)">${d.d21 >= 0 ? '+' : ''}${d.d21}일</span></div>
    <div class="tt-row"><span class="tt-label">2024 지연</span><span class="tt-val" style="color:var(--c24)">${d.d24 >= 0 ? '+' : ''}${d.d24}일</span></div>
    <div class="tt-row"><span class="tt-label">평균 지연</span><span class="tt-val">${d.avg >= 0 ? '+' : ''}${d.avg}일</span></div>
    <div class="tt-row"><span class="tt-label">예측 날짜</span><span class="tt-val" style="color:var(--cpred)">${d.pred_date} ${isPast ? '(완료)' : '(예정)'}</span></div>
    <div class="tt-row"><span class="tt-label">일관성</span><span class="tt-val">${d.consistent ? '✓ 높음' : '△ 낮음'}</span></div>
  `;
  tt.style.display = 'block';
  moveTooltip(e);
}
function moveTooltip(e) {
  tt.style.left = (e.clientX + 16) + 'px';
  tt.style.top  = (e.clientY - 10) + 'px';
}
function hideTooltip() { tt.style.display = 'none'; }

// ── Table ────────────────────────────────────────────
const tbody = document.getElementById('tableBody');
DATA.forEach(d => {
  const predDate = new Date(CL_PEAK.getTime() + d.avg * 86400000);
  const isPast   = predDate < today;
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td class="coin-name">${d.coin}</td>
    <td style="color:var(--c21)">${d.peak_21}</td>
    <td style="color:var(--c24)">${d.peak_24}</td>
    <td class="days-21">${d.d21 >= 0 ? '+' : ''}${d.d21}일</td>
    <td class="days-24">${d.d24 >= 0 ? '+' : ''}${d.d24}일</td>
    <td>${d.avg >= 0 ? '+' : ''}${d.avg}일</td>
    <td class="${isPast ? 'past' : 'pred-date future'}">${d.pred_date} ${isPast ? '' : '▶'}</td>
    <td class="${d.consistent ? 'consistent' : 'inconsistent'}">${d.consistent ? '✓ 높음' : '△ 낮음'}</td>
  `;
  tbody.appendChild(tr);
});
</script>
</body>
</html>
"""


def main():
    import os

    csv_path = DB_CSV if os.path.exists(DB_CSV) else "alt_cycle_export.csv"
    print(f"Loading: {csv_path}")
    data = prepare_data(csv_path)

    html = HTML.replace("__DATA__", json.dumps(data, ensure_ascii=False))
    out = Path(OUT_FILE)
    out.write_text(html, encoding="utf-8")
    print(f"Saved: {out.resolve()}")
    webbrowser.open(f"file://{out.resolve()}")


if __name__ == "__main__":
    main()
