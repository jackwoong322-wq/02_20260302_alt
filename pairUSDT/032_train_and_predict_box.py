"""032_train_and_predict_box.py

XGBoost 기반 다음 박스권 예측 및 DB 저장

Usage: python 032_train_and_predict_box.py
"""

import logging
import sqlite3
from datetime import datetime

import numpy as np

from lib.common.config import DB_PATH, TARGET_HI, TARGET_LO, TARGET_DUR, TARGET_PHASE
from lib.predictor.data import (
    load_box_df,
    build_training_pairs,
    build_bottom_dataset,
)
from lib.predictor.train import (
    train_box_models,
    train_bottom_models,
    print_feature_importance,
)
from lib.predictor.predict import (
    CREATE_PATHS_SQL,
    CREATE_PEAKS_SQL,
    predict_and_insert,
    print_prediction_summary,
)

log = logging.getLogger(__name__)


def main():
    log.info("=" * 65)
    log.info("032_train_and_predict_box.py 시작")
    log.info("=" * 65)

    conn = sqlite3.connect(DB_PATH)

    log.info("[1/5] 데이터 로드")
    df_all = load_box_df(conn)
    log.info("      총 %d개 박스 (is_prediction=0)", len(df_all))

    log.info("[2/7] 연속 박스 쌍 구성")
    train_df = build_training_pairs(df_all)
    log.info("      연속 쌍 수: %d개", len(train_df))

    log.info("[3/7] Bottom 학습 데이터 구성")
    bottom_df = build_bottom_dataset(df_all)
    log.info("      Bottom 샘플 수: %d개", len(bottom_df))

    log.info("[4/7] XGBoost 박스 모델 학습 (phase + BEAR/BULL별 회귀)")
    models_by_group, metrics_by_group = train_box_models(train_df)

    log.info("검증 오차(RMSE) / 정확도(Acc) 요약:")
    for grp_name, metrics in metrics_by_group.items():
        if grp_name in ("BTC", "ALT"):
            acc_ph = metrics.get(TARGET_PHASE)
            if acc_ph is not None:
                log.info("  [%s] phase     Accuracy = %.3f  (%.1f%%)", grp_name, acc_ph, acc_ph * 100)
        else:
            rmse_hi = metrics.get(TARGET_HI)
            rmse_lo = metrics.get(TARGET_LO)
            rmse_dur = metrics.get(TARGET_DUR)
            if rmse_hi is not None:
                log.info(
                    "  [%s] next_hi   RMSE = %.4f  (원래단위 오차 ≈ ±%.1f%%)",
                    grp_name, rmse_hi, float(np.expm1(rmse_hi)),
                )
            if rmse_lo is not None:
                log.info(
                    "  [%s] next_lo   RMSE = %.4f  (원래단위 오차 ≈ ±%.1f%%)",
                    grp_name, rmse_lo, float(np.expm1(rmse_lo)),
                )
            if rmse_dur is not None:
                log.info(
                    "  [%s] next_dur  RMSE = %.4f  (원래단위 오차 ≈ ±%dd)",
                    grp_name, rmse_dur, int(np.expm1(rmse_dur)),
                )

    log.info("[5/7] 피처 중요도 분석")
    print_feature_importance(models_by_group)

    log.info("[6/7] Bottom 전용 모델 학습")
    bottom_models = train_bottom_models(bottom_df)

    conn.execute(CREATE_PATHS_SQL)
    conn.execute(CREATE_PEAKS_SQL)
    conn.execute("DELETE FROM coin_prediction_paths")
    conn.execute("DELETE FROM coin_prediction_peaks")
    conn.commit()

    log.info("[7/7] 예측 실행 및 DB 저장")
    predict_and_insert(conn, df_all, train_df, models_by_group, bottom_models, {})

    print_prediction_summary(conn)

    conn.close()
    log.info("완료 — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("다음 단계: python 033_visualizer_html.py  (노란 점선 예측 박스 확인)")


if __name__ == "__main__":
    main()
