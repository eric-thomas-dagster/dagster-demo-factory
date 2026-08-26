"""The EventBridge/SQS-triggered ingestion pattern named in the POC criteria.

The AE notes call out testing "invocation from EventBridge or surface state
to Dagster" as an explicit POC criterion. The brief scopes this to a mocked
trigger pattern only -- "No EventBridge/SQS live integration -- mock the
trigger pattern, don't stand up real AWS infrastructure" -- and the registry
search for it (see `components/bronze_feed.py`'s docstring) turned up
`sqs_monitor`, which polls a real queue with no demo-mode affordance. So this
stays a small hand-written sensor: configured and visible in the UI,
default-stopped, and never touches AWS in demo mode.

Real mode would poll the SQS queue that EventBridge forwards S3
`ObjectCreated` notifications to for the credit bureau landing prefix, and
fire a `RunRequest` for the matching (date, product_line) partition the
moment a file lands -- faster than waiting for the 6am schedule.
"""

import dagster as dg

from kapitus.defs.automation.daily_bronze_ingestion_schedule import daily_bronze_ingestion_job

_DEMO_MODE = True  # No AWS credentials in this demo -- see module docstring.


@dg.sensor(
    job=daily_bronze_ingestion_job,
    minimum_interval_seconds=300,
    default_status=dg.DefaultSensorStatus.STOPPED,
    description=(
        "Polls the SQS queue behind Kapitus's EventBridge S3 put-object notifications for the "
        "credit bureau landing prefix, and fires a run for the matching partition the moment a "
        "file lands. Stopped by default -- the schedule already covers the demo narrative."
    ),
)
def credit_bureau_eventbridge_sensor(context: dg.SensorEvaluationContext) -> dg.SkipReason:
    if not _DEMO_MODE:
        raise NotImplementedError(
            "Real-mode SQS polling is not implemented in this demo. Replace this branch with a "
            "real boto3 SQS client reading Kapitus's EventBridge-fed queue."
        )
    return dg.SkipReason(
        "demo_mode: true -- no real SQS queue to poll. Set _DEMO_MODE = False and supply real "
        "AWS credentials to react to Kapitus's actual EventBridge notifications."
    )
