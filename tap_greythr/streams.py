"""Stream type classes for tap-greythr."""

from __future__ import annotations

import sys
import typing as t
import datetime
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_greythr.client import GreytHRStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class EmployeeStream(GreytHRStream):
    """Define custom stream."""

    name = "employees"
    path = "/employees"
    primary_keys = ["employeeId"]
    replication_key = "lastModified"

    schema = th.PropertiesList(
        th.Property("employeeId", th.IntegerType, description="The employee's ID"),
        th.Property("name", th.StringType, description="The employee's name"),
        th.Property("email", th.StringType, description="The employee's email address"),
        th.Property("employeeNo", th.StringType, description="The employee's number"),
        th.Property("dateOfJoin", th.StringType, description="The employee's date of joining"),
        th.Property("leavingDate", th.StringType, description="The employee's leaving date"),
        th.Property("originalHireDate", th.StringType, description="The employee's original hire date"),
        th.Property("leftorg", th.BooleanType, description="If the employee left the organization"),
        th.Property("lastModified", th.StringType, description="Last modified date"),
        th.Property("status", th.IntegerType, description="The employee's status"),
        th.Property("dateOfBirth", th.StringType, description="The employee's date of birth"),
        th.Property("gender", th.StringType, description="The employee's gender"),
        th.Property("probationPeriod", th.IntegerType, description="The employee's probation period"),
        th.Property("personalEmail2", th.StringType, description="The employee's secondary personal email"),
        th.Property("personalEmail3", th.StringType, description="The employee's tertiary personal email"),
        th.Property("mobile", th.StringType, description="The employee's mobile number"),
    ).to_dict()


class LeaveBalanceStream(GreytHRStream):
    """Define custom stream for leave balances."""

    name = "leave_balances"
    path = "/leave/v2/employee/years/{year}/balance"
    primary_keys: t.ClassVar[list[str]] = ["employeeId", "leaveTypeCategory"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("employeeId", th.IntegerType),
        th.Property("leaveTypeCategory", th.IntegerType),
        th.Property("balance", th.IntegerType),
        th.Property("ob", th.IntegerType),
        th.Property("grant", th.IntegerType),
        th.Property("availed", th.IntegerType),
        th.Property("applied", th.IntegerType),
        th.Property("lapsed", th.IntegerType),
        th.Property("deducted", th.IntegerType),
        th.Property("encashed", th.IntegerType),
    ).to_dict()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        params["size"] = 25  # Set a default page size
        return params

    def get_url(self, context: dict | None) -> str:
        """Generate the URL with dynamic year."""
        year = self.config.get("year", "2024")
        return self.url_base + self.path.format(year=year)

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the leave balances from the response."""
        data = response.json().get("data", [])
        for employee in data:
            employee_id = employee["employeeId"]
            summaries = employee.get("summaries", [])
            for summary in summaries:
                # Convert data types
                summary = {
                    "employeeId": int(employee_id),
                    "leaveTypeCategory": int(summary.get("leaveTypeCategory", 0)),
                    "balance": int(summary.get("balance", 0)),
                    "ob": int(summary.get("ob", 0)),
                    "grant": int(summary.get("grant", 0)),
                    "availed": int(summary.get("availed", 0)),
                    "applied": int(summary.get("applied", 0)),
                    "lapsed": int(summary.get("lapsed", 0)),
                    "deducted": int(summary.get("deducted", 0)),
                    "encashed": int(summary.get("encashed", 0)),
                }
                yield summary

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Optional[Any] = None,
    ) -> t.Optional[Any]:
        """Return the next page token from the response."""
        pages = response.json().get("pages", {})
        if pages.get("hasNext"):
            return (previous_token or 1) + 1
        return None


class AttendanceInsightsStream(GreytHRStream):
    """Define custom stream for attendance insights."""

    name = "attendance_insights"
    path = "/attendance/v2/employee/insights"
    primary_keys: t.ClassVar[list[int]] = ["employee"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("employee", th.IntegerType),
        th.Property("average_workHours", th.StringType),
        th.Property("average_actualWorkHours", th.StringType),
        th.Property("average_inTime", th.StringType),
        th.Property("average_outTime", th.StringType),
        th.Property("average_workHoursDiff", th.IntegerType),
        th.Property("average_actualWorkHoursDiff", th.IntegerType),
        th.Property("day_penalty", th.IntegerType),
        th.Property("day_lateIn", th.IntegerType),
        th.Property("day_earlyOut", th.IntegerType),
        th.Property("day_exception", th.IntegerType),
        # th.Property("status", th.ArrayType(
        # th.ObjectType(
        #     th.Property("type", th.StringType),
        #     th.Property("days", th.NumberType)
        #     )
        # ))
    ).to_dict()

    def get_url_params(
    self,
    context: dict | None,
    next_page_token: Any | None,
) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        params["size"] = 25  # Set a default page size
        
        # Calculate last month's start and end dates
        today = datetime.date.today()
        first_day_of_current_month = today.replace(day=1)
        
        # Calculate first day of last month
        if first_day_of_current_month.month == 1:
            first_day_of_last_month = first_day_of_current_month.replace(year=first_day_of_current_month.year - 1, month=12)
        else:
            first_day_of_last_month = first_day_of_current_month.replace(month=first_day_of_current_month.month - 1)
        
        # Calculate last day of last month
        last_day_of_last_month = first_day_of_current_month - datetime.timedelta(days=1)
        
        # Set start and end parameters
        params["start"] = self.config.get("start_date", first_day_of_last_month.isoformat())
        params["end"] = self.config.get("end_date", last_day_of_last_month.isoformat())

        return params

    def get_url(self, context: dict | None) -> str:
        """Generate the URL with dynamic year."""
        year = self.config.get("year", "2024")
        return self.url_base + self.path.format(year=year)

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the attendance insights from the response."""
        data = response.json().get("data", [])
        for record in data:
            employee_id = record["employee"]
            insights = record.get("insights", {})
            averages = insights.get("averages", [])
            days = insights.get("days", [])
            statuses = insights.get("status", [])

            result = {
                "employee": employee_id,
                "average_workHours": None,
                "average_actualWorkHours": None,
                "average_inTime": None,
                "average_outTime": None,
                "average_workHoursDiff": None,
                "average_actualWorkHoursDiff": None,
                "day_penalty": None,
                "day_lateIn": None,
                "day_earlyOut": None,
                "day_exception": None,
                "status": statuses
            }

            for average in averages:
                if average["type"] == "workHours":
                    result["average_workHours"] = average["average"]
                elif average["type"] == "actualWorkHours":
                    result["average_actualWorkHours"] = average["average"]
                elif average["type"] == "inTime":
                    result["average_inTime"] = average["average"]
                elif average["type"] == "outTime":
                    result["average_outTime"] = average["average"]
                elif average["type"] == "workHoursDiff":
                    result["average_workHoursDiff"] = average["average"]
                elif average["type"] == "actualWorkHoursDiff":
                    result["average_actualWorkHoursDiff"] = average["average"]

            for day in days:
                if day["type"] == "penalty":
                    result["day_penalty"] = day["days"]
                elif day["type"] == "lateIn":
                    result["day_lateIn"] = day["days"]
                elif day["type"] == "earlyOut":
                    result["day_earlyOut"] = day["days"]
                elif day["type"] == "exception":
                    result["day_exception"] = day["days"]

            yield result

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Optional[Any] = None,
    ) -> t.Optional[Any]:
        """Return the next page token from the response."""
        pages = response.json().get("pages", {})
        if pages.get("hasNext"):
            return (previous_token or 1) + 1
        return None