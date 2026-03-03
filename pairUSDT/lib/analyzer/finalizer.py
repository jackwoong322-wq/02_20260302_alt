def finalize_hi_lo_days(zones: list, data: list) -> list:
    for z in zones:
        if z["phase"] == "BEAR" and z.get("lo_day") is not None:
            best_hi = -float("inf")
            hi_day = z["end_x"]
            for d in data:
                if z["start_x"] <= d["x"] <= z["end_x"]:
                    if d["high"] > best_hi:
                        best_hi = d["high"]
                        hi_day = d["x"]
            z["hi_day"] = hi_day

        elif z["phase"] == "BULL" and z.get("hi_day") is not None:
            best_lo = float("inf")
            lo_day = z.get("lo_day", z["end_x"])
            for d in data:
                if z["start_x"] <= d["x"] <= z["end_x"]:
                    if d["low"] < best_lo:
                        best_lo = d["low"]
                        lo_day = d["x"]
            z["lo_day"] = lo_day

    return zones


def compute_change_pcts(zones: list, data: list) -> list:
    if not zones:
        return zones

    cycle_min_idx = zones[0]["cycle_min_idx"]
    cycle_low = data[cycle_min_idx]["low"]
    first_bull_zi = next((i for i, z in enumerate(zones) if z["phase"] == "BULL"), -1)

    for zi, z in enumerate(zones):
        prev_box = zones[zi - 1] if zi > 0 else None
        is_bear = z["phase"] == "BEAR"

        if is_bear:
            ref_low = z["lo"]
        elif zi == first_bull_zi:
            ref_low = cycle_low
        else:
            ref_low = prev_box["lo"] if prev_box else 100.0

        if is_bear:
            ref_high = prev_box["hi"] if prev_box else 100.0
        else:
            ref_high = z["hi"]

        z["hi_change_pct"] = (z["hi"] - ref_low) / ref_low * 100 if ref_low else 0.0
        z["lo_change_pct"] = (z["lo"] - ref_high) / ref_high * 100 if ref_high else 0.0

        start_close = data[0]["close"]
        if is_bear:
            z["gain_pct"] = (z["lo"] - start_close) / start_close * 100 if start_close else 0.0
        else:
            z["gain_pct"] = (z["hi"] - cycle_low) / cycle_low * 100 if cycle_low else 0.0

    return zones
