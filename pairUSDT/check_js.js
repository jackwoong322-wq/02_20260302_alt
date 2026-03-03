const fs = require('fs');
const html = fs.readFileSync('templates/chart.html', 'utf8');
const start = html.indexOf('<script>');
const end = html.indexOf('</script>', start);
if (start === -1 || end === -1) {
  console.log('Script block not found');
  process.exit(1);
}
const code = html.slice(start + 8, end).trim();
const codeToCheck = code.replace(/__CHART_DATA__/g, '{}');
fs.writeFileSync('pairUSDT/template_script.js', codeToCheck, 'utf8');
try {
  new Function(codeToCheck);
  console.log('script block: OK');
} catch (e) {
  console.log('ERROR:', e.message);
  const { execSync } = require('child_process');
  try {
    const out = execSync('node --check pairUSDT/template_script.js 2>&1', { encoding: 'utf8' });
  } catch (e2) {
    console.log(e2.stderr || e2.stdout || e2.message);
  }
  process.exit(1);
}
