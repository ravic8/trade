"""Predictive modeling and backtesting package."""

from trade_research.modeling.backtest import (
    BacktestConfig,
    BacktestRun,
    run_prediction_backtest,
)
from trade_research.modeling.baselines import (
    BaselineRun,
    BaselineRunConfig,
    run_baseline_predictions,
)
from trade_research.modeling.datasets import (
    ModelDatasetView,
    WalkForwardFold,
    fold_views,
    load_ml_dataset_v1,
    make_classification_view,
    make_downside_risk_view,
    make_prediction_view,
    make_ranking_view,
    make_regression_view,
    make_walk_forward_fold,
    trainable_rows,
)
from trade_research.modeling.lightgbm_models import (
    LightGBMRun,
    LightGBMRunConfig,
    run_lightgbm_predictions,
)
from trade_research.modeling.walk_forward import (
    WalkForwardManifestBuild,
    WalkForwardManifestConfig,
    build_walk_forward_manifest,
)

__all__ = [
    "ModelDatasetView",
    "WalkForwardFold",
    "WalkForwardManifestBuild",
    "WalkForwardManifestConfig",
    "BaselineRun",
    "BaselineRunConfig",
    "BacktestConfig",
    "BacktestRun",
    "LightGBMRun",
    "LightGBMRunConfig",
    "build_walk_forward_manifest",
    "fold_views",
    "load_ml_dataset_v1",
    "make_classification_view",
    "make_downside_risk_view",
    "make_prediction_view",
    "make_ranking_view",
    "make_regression_view",
    "make_walk_forward_fold",
    "run_baseline_predictions",
    "run_prediction_backtest",
    "run_lightgbm_predictions",
    "trainable_rows",
]
