import json
from pathlib import Path


def generate_html(data_json: dict) -> str:
    template_path = Path(__file__).resolve().parents[2] / "templates" / "chart.html"
    template = template_path.read_text(encoding="utf-8")
    json_str = json.dumps(data_json, ensure_ascii=False)
    return template.replace("__CHART_DATA__", json_str)
