const fs = require('fs');
const html = fs.readFileSync('pairUSDT/033_visualizer_html.html', 'utf8');
const idx = html.indexOf('const ALL_DATA = ');
if (idx === -1) {
  console.log('ALL_DATA NOT FOUND');
  process.exit(1);
}
let start = html.indexOf('{', idx);
let depth = 0;
let end = start;
for (let i = start; i < html.length; i++) {
  if (html[i] === '{') depth++;
  else if (html[i] === '}') { depth--; if (depth === 0) { end = i + 1; break; } }
}
const jsonStr = html.substring(start, end);
let data;
try {
  data = JSON.parse(jsonStr);
} catch (e) {
  console.log('JSON parse error:', e.message);
  process.exit(1);
}
const coins = Object.values(data);
console.log('코인 수:', coins.length);
if (coins.length === 0) {
  console.log('코인 0개');
  process.exit(1);
}
let btcBull = 0, btcBear = 0, btcPeaks = 0;
coins.forEach(c => {
  (c.cycles || []).forEach(cy => {
    const bull = cy.prediction_paths?.bull?.length ?? 0;
    const bear = cy.prediction_paths?.bear?.length ?? 0;
    const peaks = cy.peak_predictions?.length ?? 0;
    if ((c.symbol || '').toUpperCase() === 'BTC') {
      btcBull = Math.max(btcBull, bull);
      btcBear = Math.max(btcBear, bear);
      btcPeaks = Math.max(btcPeaks, peaks);
    }
  });
});
console.log('BTC bull:', btcBull, 'bear:', btcBear, 'peaks:', btcPeaks);
if (btcBull === 0 || btcBear === 0 || btcPeaks === 0) {
  process.exit(1);
}
console.log('Step 3 OK');
