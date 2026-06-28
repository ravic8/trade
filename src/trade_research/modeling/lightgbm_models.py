from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from trade_research.modeling.baselines import evaluate_prediction_metrics
from trade_research.modeling.datasets import fold_views, make_walk_forward_fold
from trade_research.modeling.ml_dataset_v1 import TARGET_COLUMN_V1
from trade_research.modeling.walk_forward import (
    WalkForwardManifestConfig,
    build_walk_forward_manifest,
)


@dataclass(frozen=True)
class LightGBMRunConfig:
    min_train_days: int = 180
    validation_days: int = 40
    prediction_step_days: int = 1
    max_folds: int | None = 10
    target_column: str = TARGET_COLUMN_V1
    top_n_values: tuple[int, ...] = (5, 10, 20)
    n_estimators: int = 80
    learning_rate: float = 0.05
    num_leaves: int = 31
    min_child_samples: int = 20
    random_state: int = 42
    n_jobs: int = 1
    include_momentum_blends: bool = True
    momentum_blend_weight: float = 0.50
    momentum_blend_feature: str = "ret_1d"


@dataclass(frozen=True)
class LightGBMRun:
    predictions: pd.DataFrame
    metrics: dict[str, Any]
    summary_md: str


def run_lightgbm_predictions(
    dataset: pd.DataFrame,
    feature_columns: list[str],
    config: LightGBMRunConfig | None = None,
) -> LightGBMRun:
    cfg = config or LightGBMRunConfig()
    lgb = _import_lightgbm()
    manifest_config = WalkForwardManifestConfig(
        min_train_days=cfg.min_train_days,
        validation_days=cfg.validation_days,
        prediction_step_days=cfg.prediction_step_days,
        max_folds=cfg.max_folds,
        target_column=cfg.target_column,
    )
    manifest = build_walk_forward_manifest(dataset, feature_columns, config=manifest_config)
    prediction_frames = []
    for fold_row in manifest.folds.to_dict(orient="records"):
        fold = make_walk_forward_fold(
            dataset,
            feature_columns,
            prediction_date=fold_row["prediction_date"],
            min_train_days=cfg.min_train_days,
            validation_days=cfg.validation_days,
        )
        train_view, validation_view, prediction_view = fold_views(
            fold,
            feature_columns,
            task="regression",
            target_column=cfg.target_column,
        )
        prediction_frames.append(
            _fold_predictions(
                lgb=lgb,
                fold=fold,
                train_view=train_view,
                validation_view=validation_view,
                prediction_view=prediction_view,
                config=cfg,
            )
        )

    predictions = (
        pd.concat(prediction_frames, ignore_index=True)
        if prediction_frames
        else _empty_predictions()
    )
    metrics = evaluate_prediction_metrics(
        predictions=predictions,
        top_n_values=cfg.top_n_values,
        manifest_summary=manifest.summary,
        config=asdict(cfg),
        artifact_name="lightgbm_predictions_v1",
    )
    return LightGBMRun(
        predictions=predictions,
        metrics=metrics,
        summary_md=_summary_markdown(metrics),
    )


def _fold_predictions(
    lgb,
    fold,
    train_view,
    validation_view,
    prediction_view,
    config: LightGBMRunConfig,
) -> pd.DataFrame:
    frames = []
    realized = fold.prediction[config.target_column].to_numpy()
    base = prediction_view.metadata.copy()
    base["prediction_date"] = fold.prediction_date
    base["fold_id"] = f"wf_{fold.prediction_date.strftime('%Y%m%d')}"
    base["train_start_date"] = fold.train_start_date
    base["train_end_date"] = fold.train_end_date
    base["validation_start_date"] = fold.validation_start_date
    base["validation_end_date"] = fold.validation_end_date
    base["realized_forward_ret_1d"] = realized

    specs = [
        (
            "lgbm_regressor",
            _fit_regressor(lgb, train_view, validation_view, config),
            "predict",
        ),
        (
            "lgbm_upside_classifier",
            _fit_classifier(
                lgb,
                fold.train,
                fold.validation,
                train_view,
                validation_view,
                config,
                "next_day_top_decile",
            ),
            "predict_proba",
        ),
        (
            "lgbm_downside_classifier",
            _fit_classifier(
                lgb,
                fold.train,
                fold.validation,
                train_view,
                validation_view,
                config,
                "next_day_bottom_decile",
            ),
            "predict_proba_inverse",
        ),
    ]
    for model_id, model, prediction_method in specs:
        out = base.copy()
        out["model_id"] = model_id
        out["score"] = _score_model(model, prediction_method, prediction_view.X)
        out["rank"] = out.groupby("prediction_date")["score"].rank(
            ascending=False,
            method="first",
        )
        for top_n in config.top_n_values:
            out[f"selected_top_{top_n}"] = out["rank"].le(top_n)
        frames.append(out)
        if config.include_momentum_blends:
            frames.append(
                _momentum_blend_predictions(
                    base=base,
                    model_id=f"{model_id}_momentum_blend",
                    model_score=out["score"],
                    prediction_frame=fold.prediction,
                    config=config,
                )
            )
    return pd.concat(frames, ignore_index=True)


def _fit_regressor(lgb, train_view, validation_view, config: LightGBMRunConfig):
    model = lgb.LGBMRegressor(
        n_estimators=config.n_estimators,
        learning_rate=config.learning_rate,
        num_leaves=config.num_leaves,
        min_child_samples=config.min_child_samples,
        random_state=config.random_state,
        n_jobs=config.n_jobs,
        verbosity=-1,
    )
    model.fit(
        train_view.X,
        train_view.y.astype(float),
        eval_set=[(validation_view.X, validation_view.y.astype(float))],
    )
    return model


def _fit_classifier(
    lgb,
    train_frame: pd.DataFrame,
    validation_frame: pd.DataFrame,
    train_view,
    validation_view,
    config: LightGBMRunConfig,
    label_column: str,
):
    model = lgb.LGBMClassifier(
        n_estimators=config.n_estimators,
        learning_rate=config.learning_rate,
        num_leaves=config.num_leaves,
        min_child_samples=config.min_child_samples,
        random_state=config.random_state,
        n_jobs=config.n_jobs,
        verbosity=-1,
    )
    train_y = train_frame[label_column].astype("boolean").astype(int)
    validation_y = validation_frame[label_column].astype("boolean").astype(int)
    model.fit(
        train_view.X,
        train_y,
        eval_set=[(validation_view.X, validation_y)],
    )
    return model


def _score_model(model, prediction_method: str, X: pd.DataFrame) -> pd.Series:
    if prediction_method == "predict":
        score = model.predict(X)
    else:
        proba = model.predict_proba(X)
        positive = proba[:, 1]
        score = 1.0 - positive if prediction_method == "predict_proba_inverse" else positive
    return pd.Series(score, index=X.index)


def _momentum_blend_predictions(
    base: pd.DataFrame,
    model_id: str,
    model_score: pd.Series,
    prediction_frame: pd.DataFrame,
    config: LightGBMRunConfig,
) -> pd.DataFrame:
    out = base.copy()
    out["model_id"] = model_id
    momentum = (
        prediction_frame[config.momentum_blend_feature]
        if config.momentum_blend_feature in prediction_frame
        else pd.Series(0.0, index=prediction_frame.index)
    )
    model_rank_score = _rank_normalize(model_score)
    momentum_rank_score = _rank_normalize(momentum)
    weight = config.momentum_blend_weight
    out["score"] = (weight * model_rank_score) + ((1.0 - weight) * momentum_rank_score)
    out["rank"] = out.groupby("prediction_date")["score"].rank(
        ascending=False,
        method="first",
    )
    for top_n in config.top_n_values:
        out[f"selected_top_{top_n}"] = out["rank"].le(top_n)
    return out


def _rank_normalize(score: pd.Series) -> pd.Series:
    if score.nunique(dropna=True) <= 1:
        return pd.Series(0.5, index=score.index)
    return score.rank(pct=True, method="average").astype(float)


def _summary_markdown(metrics: dict[str, Any]) -> str:
    lines = [
        "# LightGBM Predictions v1",
        "",
        f"Generated at: `{metrics['generated_at']}`",
        f"Prediction rows: `{metrics['prediction_row_count']}`",
        f"Models: `{metrics['model_count']}`",
        "",
        "## Model Metrics",
        "",
        "| Model | Evaluated Rows | Rank IC Mean | Top 10 Avg Return | Top 10 Hit Rate |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in metrics["models"]:
        lines.append(
            "| {model_id} | {evaluated_rows} | {rank_ic_mean} | "
            "{top_10_average_return} | {top_10_hit_rate} |".format(
                model_id=row["model_id"],
                evaluated_rows=row["evaluated_rows"],
                rank_ic_mean=_format_metric(row["rank_ic_mean"]),
                top_10_average_return=_format_metric(row.get("top_10_average_return")),
                top_10_hit_rate=_format_metric(row.get("top_10_hit_rate")),
            )
        )
    lines.append("")
    return "\n".join(lines)


def _format_metric(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):.6f}"


def _empty_predictions() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "instrument_key",
            "symbol",
            "date",
            "prediction_date",
            "fold_id",
            "model_id",
            "score",
            "rank",
            "realized_forward_ret_1d",
        ]
    )


def _import_lightgbm():
    try:
        import lightgbm as lgb
    except ImportError as exc:
        raise RuntimeError(
            "LightGBM is required for Phase 5. Install with `pip install lightgbm`."
        ) from exc
    return lgb
