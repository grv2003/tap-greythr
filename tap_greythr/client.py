from __future__ import annotations

import sys
import base64
from typing import Any, Callable, Iterable
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream
import singer_sdk.authenticators

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

# If using JSON files for schema definition:
SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

class GreytHRStream(RESTStream):
    """GreytHR stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root."""
        return "https://api.greythr.com"

    records_jsonpath = "$.data[*]"  # Adjust the JSON path to match the response format.

    @property
    def authenticator(self) -> singer_sdk.authenticators.SimpleAuthenticator:
        """Return a new authenticator object."""
        domain = self.config.get("greythr_domain")
        username = self.config.get("api_username")
        password = self.config.get("api_password")
        credentials = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode('utf-8')
        
        auth_response = requests.post(
            f'https://{domain}/uas/v1/oauth2/client-token',
            headers={
                "Authorization": f"Basic {credentials}"
            }
        )
        auth_response.raise_for_status()
        access_token = auth_response.json().get("access_token")
        return singer_sdk.authenticators.SimpleAuthenticator(
            stream=self,
            auth_headers={
                "ACCESS-TOKEN": access_token,
                "x-greythr-domain": domain
            }
        )

    @property
    def http_headers(self) -> dict:
        """Return the HTTP headers needed."""
        return self.authenticator.auth_headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance."""
        return super().get_new_paginator()

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

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request."""
        return None  # No payload required for GET requests

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure."""
        return row