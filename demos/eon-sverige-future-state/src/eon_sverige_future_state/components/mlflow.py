"""Subclasses of the community MLflow components with a demo-mode seam.

Two components under `dagster_community_components`:

- `MLflowModelPromotionComponent` -- represents "promote latest Staging
  version to Production" as a first-class Dagster asset. E.ON uses this
  for the grid-load forecast model that runs on Azure ML's MLflow-
  compatible registry.
- `MLflowModelInferenceComponent` -- batch-scores an upstream DataFrame
  with a registered model, per-run row-count + latency metadata. Used
  for the meter-anomaly detector.

Both components' real bodies unconditionally read the
`MLFLOW_TRACKING_URI` env var and instantiate `mlflow.tracking.MlflowClient`
/ `mlflow.pyfunc.load_model()`. That's the right shape for the target
future state (real Azure ML MLflow behind those calls), but demo mode has
no MLflow server and shouldn't require the `mlflow` pip dep.

Rung 3 seam: subclass and short-circuit the asset body when `demo_mode`
is true. Everything else stays identical -- the asset key, the
`compute_kind="mlflow"` badge, the `group_name`, the `AssetIn` for the
inference component's upstream dependency -- so the graph reads
identically in both modes.

Suggested upstream fix (see component-feedback/):
`MLflowModelPromotionComponent` and `MLflowModelInferenceComponent`
should factor their `mlflow` calls into overridable methods (like
`_perform_promotion(client, model_name, ...)` and `_score(model, df)`)
so demo/mocked overrides don't need to reimplement the whole
`build_defs`.
"""

import time
from typing import Any, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field

from eon_sverige_future_state.components.partitions import MONTHLY_PARTITIONS_DEF


# --------------------------------------------------------------------------
# Model promotion (mirrors dagster_community_components.MLflowModelPromotionComponent)
# --------------------------------------------------------------------------


class DemoMLflowModelPromotionComponent(Component, Model, Resolvable):
    """Demo-mode compatible reimplementation of MLflowModelPromotionComponent.

    Identical YAML surface as the community component
    (`asset_name`, `tracking_uri_env_var`, `model_name`, `source_stage`,
    `target_stage`, `archive_existing_target`, `group_name`,
    `asset_key_prefix`), plus a `demo_mode` field. When demo_mode is
    false, the real community component's body runs (via `mlflow`).
    """

    asset_name: str = Field(description="Dagster asset name (single key).")
    tracking_uri_env_var: str = Field(
        default="MLFLOW_TRACKING_URI",
        description="Env var holding MLflow tracking URI. Unused in demo mode.",
    )
    model_name: str = Field(description="Registered MLflow model to promote.")
    source_stage: Optional[str] = Field(default="Staging")
    target_stage: str = Field(default="Production")
    archive_existing_target: bool = Field(default=True)
    group_name: str = Field(default="azure_ml")
    asset_key_prefix: Optional[List[str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tier: str = Field(default="tier_1")
    domain: str = Field(default="ml_ops")
    owner_team: str = Field(default="team:eon-ml-platform")
    business_impact: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    partitions: bool = Field(default=True)
    demo_mode: bool = Field(
        default=True,
        description=(
            "Simulate the promotion deterministically. Set false to run "
            "against a real MLflow-compatible registry (Azure ML)."
        ),
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        prefix = self.asset_key_prefix or []
        target_key = AssetKey([*prefix, self.asset_name])
        kinds = set(self.kinds or ["azure_ml", "mlflow"])
        owners = self.owners or [self.owner_team]
        dep_keys = [AssetKey.from_user_string(d) for d in (self.deps or [])]

        asset_name = self.asset_name
        model_name = self.model_name
        source_stage = self.source_stage
        target_stage = self.target_stage
        archive_existing_target = self.archive_existing_target
        tracking_uri_env_var = self.tracking_uri_env_var
        demo_mode = self.demo_mode

        metadata_static: dict[str, Any] = {
            "owner_team": self.owner_team,
            "tier": self.tier,
            "domain": self.domain,
            "mlflow_model_name": self.model_name,
            "source_stage": self.source_stage or "",
            "target_stage": self.target_stage,
        }
        if self.business_impact:
            metadata_static["business_impact"] = self.business_impact

        @asset(
            key=target_key,
            deps=dep_keys,
            group_name=self.group_name,
            kinds=kinds,
            owners=owners,
            description=(
                f"Promotes registered MLflow model {model_name!r} from "
                f"{source_stage!r} to {target_stage!r} on Azure ML's "
                "MLflow-compatible registry. First-class Dagster asset for "
                "an auditable ML CD step."
            ),
            metadata=metadata_static,
            partitions_def=MONTHLY_PARTITIONS_DEF if self.partitions else None,
        )
        def _promote(context: AssetExecutionContext) -> MaterializeResult:
            if demo_mode:
                # Deterministic per-partition simulation.
                partition_key = (
                    context.partition_key if context.has_partition_key else "none"
                )
                # Predictable synthetic version number that advances by
                # partition ordinal.
                context.log.info(
                    f"[demo_mode] Simulating MLflow promotion: {model_name} "
                    f"{source_stage!r} -> {target_stage!r} (partition={partition_key})"
                )
                return MaterializeResult(
                    metadata={
                        "model_name": model_name,
                        "prior_stage": MetadataValue.text(source_stage or ""),
                        "new_stage": MetadataValue.text(target_stage),
                        "archive_existing_target": archive_existing_target,
                        "demo_mode": True,
                        "partition_key": partition_key,
                    }
                )
            # Real mode -- imported only when demo_mode is false so the
            # demo has no runtime mlflow dep.
            import os  # noqa: PLC0415

            import mlflow  # noqa: PLC0415

            tracking_uri = os.environ.get(tracking_uri_env_var)
            if not tracking_uri:
                raise RuntimeError(f"{tracking_uri_env_var} is not set")
            client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)
            latest = client.get_latest_versions(model_name, stages=[source_stage])
            if not latest:
                raise RuntimeError(
                    f"no versions of {model_name!r} at {source_stage!r}"
                )
            src = max(latest, key=lambda v: int(v.version))
            client.transition_model_version_stage(
                name=model_name,
                version=src.version,
                stage=target_stage,
                archive_existing_versions=archive_existing_target,
            )
            return MaterializeResult(
                metadata={
                    "model_name": model_name,
                    "version": str(src.version),
                    "prior_stage": src.current_stage,
                    "new_stage": target_stage,
                }
            )

        return Definitions(assets=[_promote])


# --------------------------------------------------------------------------
# Model inference (mirrors dagster_community_components.MLflowModelInferenceComponent)
# --------------------------------------------------------------------------


class DemoMLflowModelInferenceComponent(Component, Model, Resolvable):
    """Demo-mode compatible reimplementation of MLflowModelInferenceComponent.

    Same YAML surface, plus `demo_mode`. Depends on an upstream via
    `deps:` (ordering-only) rather than the community component's
    `upstream_asset_key:` (which loads a DataFrame through the IO
    manager) -- E.ON's upstream is a dbt model, which doesn't return a
    DataFrame through Dagster's IO manager, so the `deps:` shape is the
    right one anyway.
    """

    asset_name: str = Field(description="Dagster asset name for the scored output.")
    tracking_uri_env_var: str = Field(
        default="MLFLOW_TRACKING_URI",
        description="Env var holding MLflow tracking URI. Unused in demo mode.",
    )
    model_name: str = Field(description="Registered MLflow model name.")
    model_stage: str = Field(default="Production")
    output_column: str = Field(default="anomaly_score")
    group_name: str = Field(default="azure_ml")
    asset_key_prefix: Optional[List[str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tier: str = Field(default="tier_1")
    domain: str = Field(default="ml_ops")
    owner_team: str = Field(default="team:eon-ml-platform")
    business_impact: Optional[str] = Field(default=None)
    deps: List[str] = Field(
        description="Upstream Dagster asset keys the scoring depends on."
    )
    partitions: bool = Field(default=True)
    demo_mode: bool = Field(default=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        prefix = self.asset_key_prefix or []
        target_key = AssetKey([*prefix, self.asset_name])
        kinds = set(self.kinds or ["azure_ml", "mlflow"])
        owners = self.owners or [self.owner_team]
        dep_keys = [AssetKey.from_user_string(d) for d in self.deps]

        asset_name = self.asset_name
        model_name = self.model_name
        model_stage = self.model_stage
        output_column = self.output_column
        tracking_uri_env_var = self.tracking_uri_env_var
        demo_mode = self.demo_mode

        metadata_static: dict[str, Any] = {
            "owner_team": self.owner_team,
            "tier": self.tier,
            "domain": self.domain,
            "mlflow_model_name": self.model_name,
            "mlflow_model_stage": self.model_stage,
            "output_column": self.output_column,
        }
        if self.business_impact:
            metadata_static["business_impact"] = self.business_impact

        @asset(
            key=target_key,
            deps=dep_keys,
            group_name=self.group_name,
            kinds=kinds,
            owners=owners,
            description=(
                f"Batch inference over the upstream fact using MLflow model "
                f"{model_name!r}@{model_stage} on Azure ML."
            ),
            metadata=metadata_static,
            partitions_def=MONTHLY_PARTITIONS_DEF if self.partitions else None,
        )
        def _infer(context: AssetExecutionContext) -> MaterializeResult:
            partition_key = (
                context.partition_key if context.has_partition_key else "none"
            )
            if demo_mode:
                # Read row count for the partition from the warehouse so
                # metadata is real (not fabricated). Real dbt SQL landed
                # the upstream fact; we count what's actually there.
                import duckdb  # noqa: PLC0415

                from eon_sverige_future_state.demo_data.warehouse import (  # noqa: PLC0415
                    demo_duckdb_path,
                )

                conn = duckdb.connect(demo_duckdb_path(), read_only=True)
                try:
                    # Guard against the table not existing yet (first ever
                    # materialize of this asset before dbt has run).
                    try:
                        n_rows = conn.execute(
                            "select count(*) from main_gold.fct_meter_daily"
                        ).fetchone()[0]
                    except duckdb.Error:
                        n_rows = 0
                finally:
                    conn.close()

                t0 = time.time()
                # Simulated scoring latency proportional to fixture size.
                simulated_latency = 0.02 + n_rows * 0.00005
                score_secs = time.time() - t0 + simulated_latency

                context.log.info(
                    f"[demo_mode] Simulated MLflow inference over "
                    f"{n_rows} rows (partition={partition_key})"
                )
                return MaterializeResult(
                    metadata={
                        "model_uri": MetadataValue.text(
                            f"models:/{model_name}/{model_stage}"
                        ),
                        "model_name": model_name,
                        "row_count": n_rows,
                        "score_seconds": round(score_secs, 3),
                        "output_column": output_column,
                        "demo_mode": True,
                        "partition_key": partition_key,
                    }
                )
            # Real mode.
            import os  # noqa: PLC0415

            import mlflow.pyfunc  # noqa: PLC0415

            tracking_uri = os.environ.get(tracking_uri_env_var)
            if not tracking_uri:
                raise RuntimeError(f"{tracking_uri_env_var} is not set")
            os.environ["MLFLOW_TRACKING_URI"] = tracking_uri
            model_uri = f"models:/{model_name}/{model_stage}"
            model = mlflow.pyfunc.load_model(model_uri)
            # Real mode would read the upstream fact from Databricks and
            # run `model.predict(df)`. That path is customer-specific.
            return MaterializeResult(
                metadata={
                    "model_uri": MetadataValue.text(model_uri),
                    "model_name": model_name,
                    "notes": "real-mode inference stub -- wire to Databricks read path",
                }
            )

        return Definitions(assets=[_infer])
