const fs = require('fs');
const html = fs.readFileSync('pairUSDT/033_visualizer_html.html', 'utf8');
const start = html.indexOf('<script>');
const end = html.indexOf('</script>', start);
const script = html.slice(start + 8, end);
fs.writeFileSync('pairUSDT/extracted_script.js', script, 'utf8');
console.log('Extracted', script.length, 'chars');
