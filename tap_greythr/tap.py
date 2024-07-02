"""GreytHR tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_greythr import streams


class TapGreytHR(Tap):
    """GreytHR tap class."""

    name = "tap-greythr"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_username",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="Username to authenticate against the API service",
        ),
        th.Property(
            "api_password",
            th.StringType,
            required=True,
            secret=True,
            description="Password to authenticate against the API service",
        ),
        th.Property(
            "greythr_domain",
            th.StringType,
            required=True,
            description="The domain for the greytHR service, e.g., 'https://Yourcompany.greythr.com'",
        ),
        th.Property(
            "year",
            th.StringType,
            required=False,
            description="The year for which to fetch leave balances",
        ),
        th.Property(
            "start",
            th.StringType,
            required=False,
            description="Start Date to fetch Data",
        ),th.Property(
            "end",
            th.StringType,
            required=False,
            description="End DSate to fetch Data",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.GreytHRStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            # streams.EmployeeStream(self),
            streams.LeaveBalanceStream(self),
            streams.AttendanceInsightsStream(self)
        ]


if __name__ == "__main__":
    TapGreytHR.cli()
