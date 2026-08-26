"""Demo-mode resource swap for the one real network boundary this demo touches.

Follows the "Variant: components whose seam is a resource rather than a
method" pattern from `templates/demo_mode_pattern.py`. `DemoS3Resource`
subclasses the real, dagster-maintained `S3Resource` unmodified apart from
the write seam and a demo-safe default, so flipping `demo_mode: false` and
supplying real credentials is the entire migration to Kapitus's actual S3
landing bucket -- no other code change required.
"""

import logging

from dagster_aws.s3 import S3Resource
from pydantic import Field

_log = logging.getLogger(__name__)


class DemoS3Resource(S3Resource):
    """`S3Resource` that skips the real S3 upload in demo mode.

    Represents the S3 landing zone that every bronze feed writes its raw
    batch to before it's loaded into the warehouse -- Fivetran's own landing
    stage for `raw_loan_applications`, and the direct OCR/Lambda drop point
    for the other two feeds. `S3Resource.get_client()` only constructs a
    boto3 client when called, so demo mode never touches it -- there is
    nothing to eagerly authenticate against, unlike a resource that
    connects on construction.
    """

    demo_mode: bool = Field(
        default=True,
        description=(
            "Skip the real S3 upload and log the landing write instead. Set false "
            "and supply real AWS credentials to land batches in Kapitus's actual "
            "S3 bucket."
        ),
    )

    def write_landing_object(self, bucket: str, key: str, data: bytes) -> None:
        """The network seam: uploads the raw batch to the S3 landing zone."""
        if not self.demo_mode:
            self.get_client().put_object(Bucket=bucket, Key=key, Body=data)
            return
        _log.info(
            "Simulated S3 landing write: s3://%s/%s (%d bytes). Set demo_mode: false "
            "and supply real AWS credentials to land this in Kapitus's actual bucket.",
            bucket,
            key,
            len(data),
        )
