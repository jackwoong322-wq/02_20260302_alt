import math
import numpy as np


def signed_log1p(x) -> float:
    if x is None:
        return None
    return float(np.sign(x) * np.log1p(abs(x)))


def _signed_log1p(x) -> float:
    if x is None:
        return None
    x = float(x)
    return float(np.sign(x) * np.log1p(abs(x)))


def safe_log1p(x) -> float:
    if x is None or x < 0:
        return None
    return float(np.log1p(x))


def _log1p(x) -> float:
    if x is None:
        return None
    return float(np.log1p(max(float(x), 0.0)))


def safe_range_pct(hi: float, lo: float) -> float:
    if lo == 0:
        return 0.0
    return (hi - lo) / lo * 100


def _safe_div_pct(num: float, denom: float) -> float:
    return (num - denom) / denom * 100.0 if denom and denom != 0 else 0.0


def _ease_in_out(t: float) -> float:
    """일별 경로 보간용: t∈[0,1] → 부드러운 가속/감속. t*t*(3-2*t)"""
    t = max(0.0, min(1.0, float(t)))
    return t * t * (3.0 - 2.0 * t)


def _wave_offset(day: int, day_start: int, segment_days: int, amplitude_pct: float = 3.0) -> float:
    """박스 내 물결모양: 구간 내 일자에 따라 -1~1 스케일의 요동량 반환.
    segment_days 구간에 2~3파 동 정도. amplitude_pct는 구간 범위 대비 % (기본 3%)."""
    if segment_days <= 0:
        return 0.0
    progress = (day - day_start) / segment_days
    wave = math.sin(2.5 * math.pi * progress)
    return wave * (amplitude_pct / 100.0)
