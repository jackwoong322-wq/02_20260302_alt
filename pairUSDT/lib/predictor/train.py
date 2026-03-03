import logging

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, accuracy_score
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier, XGBRegressor

from lib.common.config import (
    FEATURE_COLS,
    FEATURE_COLS_BEAR,
    FEATURE_COLS_BTC_REG,
    TARGET_HI,
    TARGET_LO,
    TARGET_DUR,
    TARGET_PHASE,
    BOX_FEATURE_WEIGHTS,
    XGB_REG_PARAMS,
    XGB_REG_PARAMS_BTC,
    XGB_CLF_PARAMS,
)

log = logging.getLogger(__name__)


def train_bottom_models(df_bottom: pd.DataFrame):
    models_by_group: dict[str, dict[str, object]] = {}

    if df_bottom.empty:
        return models_by_group

    for group_name, selector in (("BTC", lambda s: s.upper() == "BTC"), ("ALT", lambda s: s.upper() != "BTC")):
        sub = df_bottom[df_bottom["symbol"].apply(selector)]
        sub = sub.dropna(subset=FEATURE_COLS + ["bottom_norm_lo", "bottom_day", "trend_label"])
        if len(sub) < 30:
            continue

        X = sub[FEATURE_COLS]

        mdl_lo = XGBRegressor(**XGB_REG_PARAMS)
        mdl_lo.fit(X, sub["bottom_norm_lo"])

        mdl_day = XGBRegressor(**XGB_REG_PARAMS)
        mdl_day.fit(X, sub["bottom_day"])

        mdl_trend = XGBClassifier(**XGB_CLF_PARAMS)
        mdl_trend.fit(X, sub["trend_label"])

        models_by_group[group_name] = {
            "bottom_lo": mdl_lo,
            "bottom_day": mdl_day,
            "trend": mdl_trend,
        }

        log.info("[Bottom] group=%s samples=%d", group_name, len(sub))

    return models_by_group


def train_box_models(train_df: pd.DataFrame):
    all_tgt = [TARGET_HI, TARGET_LO, TARGET_DUR, TARGET_PHASE]
    df = train_df.dropna(subset=FEATURE_COLS + all_tgt).copy()

    for col, w in BOX_FEATURE_WEIGHTS.items():
        if col in df.columns:
            df[col] = df[col].astype(float) * w

    n = len(df)
    log.info("학습 샘플 수: %d  (전체 쌍 %d 중 유효)", n, len(train_df))
    if n < 10:
        raise ValueError(f"학습 데이터 부족: {n}개 (최소 30개 필요)")

    sym_col = "symbol" if "symbol" in df.columns else "meta_symbol"
    models_by_group: dict[str, dict] = {}
    metrics_by_group: dict[str, dict] = {}

    # 1) Phase 분류기: BTC / ALT (다음 박스가 BULL인지 BEAR인지)
    for group_name, selector in (("BTC", lambda s: str(s).upper() == "BTC"), ("ALT", lambda s: str(s).upper() != "BTC")):
        sub = df[df[sym_col].apply(selector)]
        if len(sub) < 10:
            log.warning("[BoxPhase] group=%s 샘플 부족 (%d개) → 스킵", group_name, len(sub))
            continue
        X = sub[FEATURE_COLS]
        y_ph = sub[TARGET_PHASE]
        X_tr, X_va, y_tr, y_va = train_test_split(X, y_ph, test_size=0.2, random_state=42)
        clf = XGBClassifier(**XGB_CLF_PARAMS)
        clf.fit(X_tr, y_tr)
        acc = accuracy_score(y_va, clf.predict(X_va))
        models_by_group[group_name] = {TARGET_PHASE: clf}
        metrics_by_group[group_name] = {TARGET_PHASE: acc}
        log.info("  [%s]  [phase] val Accuracy = %.3f", group_name, acc)

    # 2) Box 회귀(hi/lo/dur): BEAR / BULL 별로 4개 그룹
    for group_name, selector in (
        ("BTC_BEAR", lambda r: str(r[sym_col]).upper() == "BTC" and r[TARGET_PHASE] == 0),
        ("BTC_BULL", lambda r: str(r[sym_col]).upper() == "BTC" and r[TARGET_PHASE] == 1),
        ("ALT_BEAR", lambda r: str(r[sym_col]).upper() != "BTC" and r[TARGET_PHASE] == 0),
        ("ALT_BULL", lambda r: str(r[sym_col]).upper() != "BTC" and r[TARGET_PHASE] == 1),
    ):
        sub = df[df.apply(selector, axis=1)]
        if len(sub) < 10:
            log.warning("[BoxReg] group=%s 샘플 부족 (%d개) → 스킵", group_name, len(sub))
            continue
        if group_name in ("BTC_BEAR", "BTC_BULL"):
            feat_cols = FEATURE_COLS_BTC_REG
            reg_params = XGB_REG_PARAMS_BTC
        else:
            feat_cols = FEATURE_COLS_BEAR if group_name.endswith("_BEAR") else FEATURE_COLS
            reg_params = XGB_REG_PARAMS
        X = sub[feat_cols]
        models = {}
        metrics = {}
        for tgt in [TARGET_HI, TARGET_LO, TARGET_DUR]:
            y = sub[tgt]
            X_tr, X_va, y_tr, y_va = train_test_split(X, y, test_size=0.2, random_state=42)
            mdl = XGBRegressor(**reg_params)
            mdl.fit(X_tr, y_tr)
            rmse = np.sqrt(mean_squared_error(y_va, mdl.predict(X_va)))
            approx_unit = float(np.expm1(rmse))
            models[tgt] = mdl
            metrics[tgt] = rmse
            log.info("  [%s]  [%s] val RMSE = %.4f  (원래단위 오차 ≈ %.2f%%)", group_name, tgt, rmse, approx_unit)
        sigma = {}
        for tgt in [TARGET_HI, TARGET_LO, TARGET_DUR]:
            X_s, X_v, y_s, y_v = train_test_split(sub[feat_cols], sub[tgt], test_size=0.2, random_state=42)
            resid = y_v.values - models[tgt].predict(X_v)
            sigma[tgt] = float(np.std(resid))
        models["sigma"] = sigma
        log.info("  [%s]  σ 계산 완료: hi=%.4f  lo=%.4f  dur=%.4f", group_name, sigma[TARGET_HI], sigma[TARGET_LO], sigma[TARGET_DUR])
        models_by_group[group_name] = models
        metrics_by_group[group_name] = metrics
        log.info("[BoxReg] group=%s samples=%d", group_name, len(sub))

    return models_by_group, metrics_by_group


def print_feature_importance(models_by_group: dict):
    """models_by_group: phase용 "BTC"/"ALT", 회귀용 "BTC_BEAR"/"BTC_BULL"/"ALT_BEAR"/"ALT_BULL" """
    log.info("=" * 65)
    log.info("피처 중요도 (Feature Importance)  ※ gain 기준")
    for group_name in ("BTC", "ALT"):
        models = models_by_group.get(group_name)
        if not models or TARGET_PHASE not in models:
            continue
        log.info("%-25s  %7s  [%s phase]", "Feature", "phase", group_name)
        log.info("-" * 65)
        fi = models[TARGET_PHASE].feature_importances_
        imps = dict(zip(FEATURE_COLS, fi))
        for f in sorted(FEATURE_COLS, key=lambda x: imps.get(x, 0), reverse=True):
            log.info("  %-25s  %7.4f   %s", f, imps.get(f, 0), "█" * int(imps.get(f, 0) * 60))
        log.info("=" * 65)
    for group_name in ("BTC_BEAR", "BTC_BULL", "ALT_BEAR", "ALT_BULL"):
        models = models_by_group.get(group_name)
        if not models or TARGET_HI not in models:
            continue
        feat_cols_reg = FEATURE_COLS_BTC_REG if group_name in ("BTC_BEAR", "BTC_BULL") else (FEATURE_COLS_BEAR if group_name.endswith("_BEAR") else FEATURE_COLS)
        log.info("%-25s  %7s  %7s  %7s  [%s]", "Feature", "next_hi", "next_lo", "next_dur", group_name)
        log.info("-" * 65)
        imps = {}
        for tgt in [TARGET_HI, TARGET_LO, TARGET_DUR]:
            fi = models[tgt].feature_importances_
            imps[tgt] = dict(zip(feat_cols_reg, fi))
        combined = {f: sum(imps[t].get(f, 0) for t in imps) / 3 for f in feat_cols_reg}
        for f in sorted(feat_cols_reg, key=lambda x: combined.get(x, 0), reverse=True):
            log.info(
                "  %-25s  %7.4f  %7.4f  %7.4f   %s",
                f,
                imps[TARGET_HI].get(f, 0),
                imps[TARGET_LO].get(f, 0),
                imps[TARGET_DUR].get(f, 0),
                "█" * int(combined.get(f, 0) * 60),
            )
        log.info("=" * 65)


def get_feature_importance(models_by_group: dict) -> dict:
    """피처 중요도를 그룹별·타깃별 dict/DataFrame으로 반환. (훈련 결과 확인·저장용)"""
    import pandas as pd
    out = {}
    for group_name in ("BTC", "ALT"):
        models = models_by_group.get(group_name)
        if not models or TARGET_PHASE not in models:
            continue
        fi = models[TARGET_PHASE].feature_importances_
        out[group_name] = {"phase": pd.Series(dict(zip(FEATURE_COLS, fi))).sort_values(ascending=False)}
    for group_name in ("BTC_BEAR", "BTC_BULL", "ALT_BEAR", "ALT_BULL"):
        models = models_by_group.get(group_name)
        if not models or TARGET_HI not in models:
            continue
        feat_cols_reg = FEATURE_COLS_BTC_REG if group_name in ("BTC_BEAR", "BTC_BULL") else (FEATURE_COLS_BEAR if group_name.endswith("_BEAR") else FEATURE_COLS)
        imps = {}
        for tgt in [TARGET_HI, TARGET_LO, TARGET_DUR]:
            fi = models[tgt].feature_importances_
            imps[tgt] = dict(zip(feat_cols_reg, fi))
        combined = {f: sum(imps[t].get(f, 0) for t in imps) / 3 for f in feat_cols_reg}
        out[group_name] = {
            TARGET_HI: pd.Series(imps[TARGET_HI]).sort_values(ascending=False),
            TARGET_LO: pd.Series(imps[TARGET_LO]).sort_values(ascending=False),
            TARGET_DUR: pd.Series(imps[TARGET_DUR]).sort_values(ascending=False),
            "combined": pd.Series(combined).sort_values(ascending=False),
        }
    return out
