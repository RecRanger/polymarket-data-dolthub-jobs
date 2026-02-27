"""Tools to make URL requests."""

from typing import Any

import backoff
import orjson
import requests


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def url_get_request(url: str) -> Any:  # noqa: ANN401  # pyright: ignore[reportExplicitAny, reportAny]
    """Make a URL request and return the response as a dictionary."""
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return orjson.loads(response.content)  # pyright: ignore[reportAny]
