"""
BTC 반감기 기준 알트코인 Peak 타임라인
- 반감기 대비 peak 일수를 사이클별로 비교
- 추세(빨라지는 정도)를 반영한 다음 사이클 예측
"""

import pandas as pd
import numpy as np
import json
import webbrowser
from pathlib import Path

CSV_PATH = "alt_cycle_export.csv"
OUT_FILE = "peak_timeline.html"

HALVINGS = {
    "2017era": pd.Timestamp("2016-07-09"),
    "2021era": pd.Timestamp("2020-05-11"),
    "2024era": pd.Timestamp("2024-04-20"),
    "2028era": pd.Timestamp("2028-03-20"),
}
LABELS = {
    "tether": "USDT",
    "chainlink": "LINK",
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
    "bitcoin-cash": "BCH",
    "stellar": "XLM",
    "cardano": "ADA",
    "litecoin": "LTC",
}


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
        if dt.year <= 2018:
            return "2017era"
        if dt.year <= 2022:
            return "2021era"
        return "2024era"

    peaks["era"] = peaks["peak_dt"].apply(era)
    peaks["halving"] = peaks["era"].map(HALVINGS)
    peaks["days_from_halving"] = (peaks["peak_dt"] - peaks["halving"]).dt.days

    era21 = peaks[peaks["era"] == "2021era"].set_index("coin_id")
    era24 = peaks[peaks["era"] == "2024era"].set_index("coin_id")
    common = set(era21.index) & set(era24.index)

    H28 = HALVINGS["2028era"]
    timeline = []
    for coin in common:
        d21 = int(era21.loc[coin, "days_from_halving"])
        d24 = int(era24.loc[coin, "days_from_halving"])
        trend = d24 - d21
        d28 = d24 + trend
        pred = (H28 + pd.Timedelta(days=d28)).strftime("%Y-%m-%d")
        timeline.append(
            {
                "coin": LABELS.get(coin, coin.upper().split("-")[0]),
                "coin_id": coin,
                "d21": d21,
                "d24": d24,
                "trend": trend,
                "d28": d28,
                "pred_date": pred,
                "consistent": abs(trend) < 150,
                "peak_21": era21.loc[coin, "peak_date"],
                "peak_24": era24.loc[coin, "peak_date"],
            }
        )

    timeline.sort(key=lambda x: x["d24"])
    return {
        "data": timeline,
        "halvings": {k: v.strftime("%Y-%m-%d") for k, v in HALVINGS.items()},
    }


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Altcoin Peak Timeline</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@700;800&display=swap');
:root {
  --bg:    #080c14; --panel: #0d1420; --border: #1e2d45;
  --accent:#00d4ff; --text:  #c8d8f0; --dim:    #4a6080;
  --green: #00ff88; --gold:  #FFB800; --red:    #ff4466;
  --c21:   #4fc3f7; --c24:   #FFB800; --c28:    #ff6b9d;
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
  display:flex; gap:20px; align-items:center; flex-wrap:wrap;
  padding:12px 32px; border-bottom:1px solid var(--border); background:var(--panel);
}
.leg { display:flex; align-items:center; gap:8px; font-size:11px; }
.leg-dot { width:12px; height:12px; border-radius:50%; flex-shrink:0; }
.leg-line { width:24px; height:3px; border-radius:2px; flex-shrink:0; }

/* ── Summary Cards ── */
.cards { display:flex; gap:1px; border-bottom:1px solid var(--border); }
.card {
  flex:1; padding:16px 20px; background:var(--panel);
  border-right:1px solid var(--border);
}
.card:last-child { border-right:none; }
.card-label { font-size:9px; color:var(--dim); letter-spacing:1.5px; margin-bottom:6px; }
.card-value { font-size:15px; font-weight:600; font-family:'Syne',sans-serif; }
.card-sub   { font-size:10px; color:var(--dim); margin-top:3px; }

/* ── Chart ── */
.chart-section { padding:28px 32px 0; }
.section-title {
  font-family:'Syne',sans-serif; font-size:13px; font-weight:700;
  color:white; letter-spacing:2px; margin-bottom:20px;
}

.chart-wrap { position:relative; overflow-x:auto; padding-bottom:20px; }
.axis-row {
  display:flex; align-items:flex-end; margin-left:90px;
  margin-bottom:4px; position:relative; height:28px;
}
.coin-row {
  display:flex; align-items:center;
  height:56px; border-bottom:1px solid rgba(30,45,69,0.35);
  transition:background 0.12s;
}
.coin-row:hover { background:rgba(0,212,255,0.03); }
.coin-label {
  width:90px; min-width:90px; text-align:right;
  padding-right:14px; font-size:12px; font-weight:600; color:white;
}
.track { flex:1; position:relative; height:100%; }

/* dots & lines */
.dot {
  position:absolute; top:50%; transform:translate(-50%,-50%);
  border-radius:50%; cursor:pointer; z-index:3;
  transition:transform 0.15s;
}
.dot:hover { transform:translate(-50%,-50%) scale(1.6); z-index:10; }
.d21  { width:11px; height:11px; background:var(--c21); }
.d24  { width:13px; height:13px; background:var(--c24); }
.d28  { width:11px; height:11px; background:transparent; border:2.5px dashed var(--c28); }

.trend-arrow {
  position:absolute; top:50%; height:2px;
  transform:translateY(-50%); z-index:1; border-radius:1px;
}
.trend-label {
  position:absolute; top:25%; font-size:9px;
  transform:translateX(-50%); color:var(--c28);
  font-weight:600; white-space:nowrap;
}

/* axis */
.axis-tick {
  position:absolute; top:0; bottom:0;
  width:1px; background:var(--border); opacity:0.5;
}
.axis-label {
  position:absolute; font-size:9px; color:var(--dim);
  transform:translateX(-50%); bottom:0; white-space:nowrap;
}
.halving-line {
  position:absolute; top:0; bottom:0;
  width:2px; z-index:2;
}
.halving-label {
  position:absolute; font-size:9px; font-weight:600;
  transform:translateX(-50%); bottom:0; white-space:nowrap;
  padding:2px 6px; border-radius:3px;
}

/* tooltip */
.tt {
  position:fixed; background:rgba(8,12,20,0.96);
  border:1px solid var(--border); border-radius:8px;
  padding:12px 16px; font-size:11px; pointer-events:none;
  z-index:999; display:none; backdrop-filter:blur(10px); min-width:200px;
}
.tt-title { font-weight:700; color:white; font-size:13px; margin-bottom:8px;
            font-family:'Syne',sans-serif; }
.tt-row { display:flex; justify-content:space-between; gap:20px; margin:4px 0; }
.tt-lbl { color:var(--dim); }
.tt-val { font-weight:600; }

/* ── Table ── */
.table-section { padding:28px 32px 48px; }
table { width:100%; border-collapse:collapse; font-size:11px; }
th {
  padding:9px 14px; text-align:left; font-size:9px; letter-spacing:1.5px;
  color:var(--dim); border-bottom:1px solid var(--border);
}
td { padding:11px 14px; border-bottom:1px solid rgba(30,45,69,0.35); }
tr:hover td { background:rgba(0,212,255,0.03); }
.cn { font-weight:600; color:white; font-size:13px; }
.future { color:var(--green); font-weight:600; }
.past   { color:var(--dim); }
.speeding { color:var(--red); }
.slowing  { color:var(--gold); }
.stable   { color:var(--green); }
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="title">ALT PEAK <span>ROTATION</span></div>
    <div class="subtitle">BTC HALVING 기준 · 사이클별 Peak 선후행 분석 + 2028 예측</div>
  </div>
  <div style="font-size:11px;color:var(--dim);text-align:right">
    <div>5차 반감기 예상</div>
    <div style="color:var(--c28);font-size:14px;font-weight:600;margin-top:4px">2028-03-20</div>
  </div>
</div>

<div class="legend-bar">
  <div class="leg"><div class="leg-dot" style="background:var(--c21)"></div> 2021 사이클 실제 peak</div>
  <div class="leg"><div class="leg-dot" style="background:var(--c24)"></div> 2024 사이클 실제 peak</div>
  <div class="leg"><div class="leg-dot" style="background:transparent;border:2.5px dashed var(--c28);border-radius:50%"></div> 2028 사이클 예측 peak</div>
  <div class="leg"><div class="leg-line" style="background:linear-gradient(90deg,var(--c24),var(--c28))"></div> 추세 방향</div>
  <div style="margin-left:auto;font-size:10px;color:var(--dim)">x축 = 반감기 기준 경과일 (음수=반감기 이전)</div>
</div>

<div class="cards" id="cards"></div>

<div class="chart-section">
  <div class="section-title">HALVING 기준 PEAK 타임라인</div>
  <div class="chart-wrap">
    <div class="axis-row" id="axisRow" style="width:1100px"></div>
    <div id="chartRows" style="width:1100px;position:relative"></div>
  </div>
</div>

<div class="table-section">
  <div class="section-title">상세 데이터 + 2028 예측</div>
  <table>
    <thead>
      <tr>
        <th>코인</th>
        <th style="color:var(--c21)">2021 PEAK</th>
        <th style="color:var(--c24)">2024 PEAK</th>
        <th style="color:var(--c21)">반감기+일 (2021)</th>
        <th style="color:var(--c24)">반감기+일 (2024)</th>
        <th>추세 (사이클마다)</th>
        <th style="color:var(--c28)">2028 예측 PEAK</th>
        <th style="color:var(--c28)">반감기+일 (2028)</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<div class="tt" id="tt"></div>

<script>
const RAW  = __DATA__;
const DATA = RAW.data;
const today = new Date();

// ── Summary Cards ──────────────────────────────────
const cards = document.getElementById('cards');
const avgTrend = Math.round(DATA.reduce((s,d)=>s+d.trend,0)/DATA.length);
const fastCoin = DATA.reduce((a,b)=>a.trend<b.trend?a:b);
const slowCoin = DATA.reduce((a,b)=>a.trend>b.trend?a:b);
cards.innerHTML = `
  <div class="card">
    <div class="card-label">분석 코인</div>
    <div class="card-value" style="color:var(--accent)">${DATA.length}개</div>
    <div class="card-sub">2021+2024 공통</div>
  </div>
  <div class="card">
    <div class="card-label">평균 가속 추세</div>
    <div class="card-value" style="color:var(--red)">${avgTrend}일/사이클</div>
    <div class="card-sub">음수 = 빨라지는 중</div>
  </div>
  <div class="card">
    <div class="card-label">가장 빨라진 코인</div>
    <div class="card-value" style="color:var(--c28)">${fastCoin.coin}</div>
    <div class="card-sub">${fastCoin.trend}일/사이클</div>
  </div>
  <div class="card">
    <div class="card-label">가장 안정적인 코인</div>
    <div class="card-value" style="color:var(--green)">${DATA.find(d=>d.consistent)||DATA[0] ? DATA.filter(d=>d.consistent).sort((a,b)=>Math.abs(a.trend)-Math.abs(b.trend))[0]?.coin : '-'}</div>
    <div class="card-sub">ETH: ${DATA.find(d=>d.coin_id==='ethereum')?.trend||'?'}일/사이클</div>
  </div>
  <div class="card">
    <div class="card-label">2028 반감기</div>
    <div class="card-value" style="color:var(--c28)">2028-03-20</div>
    <div class="card-sub">5차 (예상)</div>
  </div>
`;

// ── Chart ──────────────────────────────────────────
const MIN_D = Math.min(...DATA.map(d=>Math.min(d.d21,d.d24,d.d28))) - 60;
const MAX_D = Math.max(...DATA.map(d=>Math.max(d.d21,d.d24,d.d28))) + 60;
const RANGE  = MAX_D - MIN_D;
const TW     = 1010; // track width

function px(d) { return ((d-MIN_D)/RANGE*TW); }

// axis ticks
const axisRow   = document.getElementById('axisRow');
const chartRows = document.getElementById('chartRows');
axisRow.style.position = 'relative';

for(let d = Math.ceil(MIN_D/100)*100; d <= MAX_D; d += 100) {
  const line = document.createElement('div');
  line.className = 'axis-tick';
  line.style.left = (90 + px(d)) + 'px';
  line.style.top  = '0';
  line.style.bottom = '0';
  line.style.position = 'absolute';
  chartRows.appendChild(line);

  const lbl = document.createElement('div');
  lbl.className = 'axis-label';
  lbl.style.left = (90 + px(d)) + 'px';
  lbl.style.position = 'absolute';
  lbl.textContent = d === 0 ? 'HALVING' : (d>0?`+${d}d`:`${d}d`);
  lbl.style.color = d===0 ? 'var(--accent)' : '';
  lbl.style.fontWeight = d===0 ? '600' : '';
  axisRow.appendChild(lbl);
}

// coin rows
DATA.forEach(d => {
  const row = document.createElement('div');
  row.className = 'coin-row';

  const lbl = document.createElement('div');
  lbl.className = 'coin-label';
  lbl.textContent = d.coin;
  row.appendChild(lbl);

  const track = document.createElement('div');
  track.className = 'track';
  track.style.width = TW + 'px';
  track.style.position = 'relative';

  // trend arrow (d24 → d28)
  const minX = Math.min(px(d.d24), px(d.d28));
  const maxX = Math.max(px(d.d24), px(d.d28));
  const arr = document.createElement('div');
  arr.className = 'trend-arrow';
  arr.style.left  = minX + 'px';
  arr.style.width = (maxX - minX) + 'px';
  arr.style.background = 'linear-gradient(90deg, var(--c24), var(--c28))';
  arr.style.opacity = '0.5';
  track.appendChild(arr);

  // trend label
  const trendLbl = document.createElement('div');
  trendLbl.className = 'trend-label';
  trendLbl.style.left = ((px(d.d24) + px(d.d28))/2) + 'px';
  trendLbl.textContent = `${d.trend>0?'+':''}${d.trend}d`;
  track.appendChild(trendLbl);

  // dots
  addDot(track, d.d21, 'd21', d, '2021');
  addDot(track, d.d24, 'd24', d, '2024');
  addDot(track, d.d28, 'd28', d, '2028');

  row.appendChild(track);
  chartRows.appendChild(row);
});

function addDot(parent, days, cls, d, era) {
  const dot = document.createElement('div');
  dot.className = 'dot ' + cls;
  dot.style.left = px(days) + 'px';
  dot.addEventListener('mouseenter', e => showTT(e, d, era));
  dot.addEventListener('mousemove',  e => moveTT(e));
  dot.addEventListener('mouseleave', () => document.getElementById('tt').style.display='none');
  parent.appendChild(dot);
}

// tooltip
const tt = document.getElementById('tt');
function showTT(e, d, era) {
  const predDt  = new Date(d.pred_date);
  const isPast  = predDt < today;
  const daysVal = era==='2021' ? d.d21 : era==='2024' ? d.d24 : d.d28;
  const peakVal = era==='2021' ? d.peak_21 : era==='2024' ? d.peak_24 : d.pred_date;
  tt.innerHTML = `
    <div class="tt-title">${d.coin} · ${era} 사이클</div>
    <div class="tt-row"><span class="tt-lbl">Peak 날짜</span><span class="tt-val" style="color:${era==='2021'?'var(--c21)':era==='2024'?'var(--c24)':'var(--c28)'}">${peakVal}${era==='2028'&&isPast?' ✓완료':era==='2028'?' ▶예정':''}</span></div>
    <div class="tt-row"><span class="tt-lbl">반감기+일</span><span class="tt-val">${daysVal>=0?'+':''}${daysVal}일</span></div>
    <div class="tt-row"><span class="tt-lbl">추세</span><span class="tt-val" style="color:${d.trend<0?'var(--red)':'var(--gold)'}">${d.trend>0?'+':''}${d.trend}일/사이클</span></div>
    <div class="tt-row"><span class="tt-lbl">2021 peak</span><span class="tt-val" style="color:var(--c21)">${d.peak_21}</span></div>
    <div class="tt-row"><span class="tt-lbl">2024 peak</span><span class="tt-val" style="color:var(--c24)">${d.peak_24}</span></div>
    <div class="tt-row"><span class="tt-lbl">2028 예측</span><span class="tt-val" style="color:var(--c28)">${d.pred_date}</span></div>
  `;
  tt.style.display = 'block';
  moveTT(e);
}
function moveTT(e) {
  tt.style.left = (e.clientX+16)+'px';
  tt.style.top  = (e.clientY-10)+'px';
}

// ── Table ──────────────────────────────────────────
const tbody = document.getElementById('tbody');
DATA.forEach(d => {
  const predDt = new Date(d.pred_date);
  const isPast = predDt < today;
  const trendCls = d.trend < -150 ? 'speeding' : d.trend > 50 ? 'slowing' : 'stable';
  const trendStr = `${d.trend>0?'+':''}${d.trend}일`;
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td class="cn">${d.coin}</td>
    <td style="color:var(--c21)">${d.peak_21}</td>
    <td style="color:var(--c24)">${d.peak_24}</td>
    <td style="color:var(--c21)">${d.d21>=0?'+':''}${d.d21}일</td>
    <td style="color:var(--c24)">${d.d24>=0?'+':''}${d.d24}일</td>
    <td class="${trendCls}">${trendStr}/사이클</td>
    <td class="${isPast?'past':'future'}">${d.pred_date} ${isPast?'✓':'▶'}</td>
    <td style="color:var(--c28)">${d.d28>=0?'+':''}${d.d28}일</td>
  `;
  tbody.appendChild(tr);
});
</script>
</body>
</html>"""


def main():
    import os

    csv = CSV_PATH if os.path.exists(CSV_PATH) else "alt_cycle_export.csv"
    print(f"Loading: {csv}")
    data = prepare_data(csv)

    html = HTML.replace("__DATA__", json.dumps(data, ensure_ascii=False))
    out = Path(OUT_FILE)
    out.write_text(html, encoding="utf-8")
    print(f"Saved: {out.resolve()}")
    webbrowser.open(f"file://{out.resolve()}")


if __name__ == "__main__":
    main()
