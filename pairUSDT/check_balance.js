const fs = require('fs');
const html = fs.readFileSync('templates/chart.html', 'utf8');
const start = html.indexOf('<script>');
const end = html.indexOf('</script>', start);
const code = html.slice(start + 8, end).replace(/__CHART_DATA__/g, '{}');
let p = 0, b = 0, s = 0;
let inStr = null, escape = false;
let i = 0;
for (; i < code.length; i++) {
  const c = code[i];
  if (escape) { escape = false; continue; }
  if (inStr) {
    if (c === '\\') escape = true;
    else if (c === inStr) inStr = null;
    continue;
  }
  if (c === '"' || c === "'" || c === '`') inStr = c;
  else if (c === '(') p++;
  else if (c === ')') { p--; if (p < 0) { console.log('Extra ) at char', i, 'line', code.slice(0,i).split('\n').length); break; } }
  else if (c === '[') b++;
  else if (c === ']') b--;
  else if (c === '{') s++;
  else if (c === '}') { s--; if (s < 0) { console.log('Extra } at char', i, 'line', code.slice(0,i).split('\n').length); break; } }
}
console.log('Final ( ) [ ] { } :', p, b, s);
if (p > 0) console.log('Unclosed ( count:', p);
const lines = code.split('\n');
console.log('Total lines:', lines.length);
