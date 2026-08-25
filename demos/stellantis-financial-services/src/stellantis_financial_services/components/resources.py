"""Demo-mode resource swaps for the two real network boundaries this demo touches.

Follows the "Variant: components whose seam is a resource rather than a
method" pattern from `templates/demo_mode_pattern.py`. Both resources
subclass the real, dagster-maintained integration unmodified apart from the
connection seam and demo-safe defaults, so flipping `demo_mode: false` and
supplying real credentials is the entire migration to SFS's actual Azure
tenant and Teams channel -- no other code change required.
"""

import logging
from typing import Any

from dagster_azure.adls2 import ADLS2Key, ADLS2Resource
from dagster_msteams import MSTeamsResource
from pydantic import Field

_log = logging.getLogger(__name__)


class DemoADLS2Resource(ADLS2Resource):
    """`ADLS2Resource` that skips the real Azure Data Lake connection in demo mode.

    Represents the OneLake/ADLS-compatible landing zone under Fabric that
    bronze vendor files land in before being loaded into the warehouse.
    Every field has a demo-safe default so this resource never requires a
    manual credential to run the demo -- flipping `demo_mode: false` and
    supplying a real `storage_account`/`credential` is the entire migration
    to SFS's actual OneLake/ADLS account.
    """

    storage_account: str = Field(
        default="sfsdemo",
        description="Azure storage account name (unused in demo mode).",
    )
    credential: Any = Field(
        default_factory=lambda: ADLS2Key(key="demo-mode-unused"),
        description="Azure storage credential (unused in demo mode).",
    )
    demo_mode: bool = Field(
        default=True,
        description=(
            "Skip the real ADLS2 upload and log the landing write instead. "
            "Set false and supply a real storage_account + credential to land "
            "vendor files in SFS's actual OneLake/ADLS account."
        ),
    )

    def write_landing_blob(self, container: str, path: str, data: bytes) -> None:
        """The network seam: uploads the raw vendor file to the ADLS landing zone.

        Demo mode never constructs a real Azure client -- `adls2_client` is a
        cached property on the base class that eagerly authenticates on first
        access, so skipping the call entirely (rather than calling it and
        discarding the result) is what keeps demo mode credential-free.
        """
        if not self.demo_mode:
            file_client = self.adls2_client.get_file_system_client(container).get_file_client(path)
            file_client.upload_data(data, overwrite=True)
            return
        _log.info(
            "Simulated ADLS landing write: %s/%s (%d bytes). Set demo_mode: false "
            "and supply real Azure credentials to land this in OneLake.",
            container,
            path,
            len(data),
        )


class _DemoTeamsClient:
    """Records the payload instead of posting to a real Teams webhook."""

    def is_legacy_webhook(self) -> bool:
        return False

    def post_message(self, payload: Any) -> bool:
        _log.info("Simulated Teams post (demo_mode: true, no webhook called): %s", payload)
        return True


class DemoMSTeamsResource(MSTeamsResource):
    """`MSTeamsResource` that logs instead of posting in demo mode.

    `hook_url` has no safe default in the base class -- it is required with
    no default, which would violate the zero-setup rule. This subclass gives
    it a demo-safe placeholder that is never dereferenced when `demo_mode` is
    true, since `get_client()` returns a logging stand-in before the real
    `TeamsClient` (and its webhook URL) is ever touched.
    """

    hook_url: str = Field(
        default="https://demo-mode-no-webhook-configured.example/webhook",
        description="Teams incoming webhook URL (unused in demo mode).",
    )
    demo_mode: bool = Field(
        default=True,
        description=(
            "Log the alert instead of posting to a real Teams channel. Set "
            "false and supply a real hook_url to post to SFS's actual Teams "
            "channel -- no other code change required."
        ),
    )

    def get_client(self):
        if not self.demo_mode:
            return super().get_client()
        return _DemoTeamsClient()
