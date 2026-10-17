from __future__ import annotations

import math
from contextlib import nullcontext
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable
from urllib.error import HTTPError

from ._client_lifetime import _Lifetime, _transport_failure
from ._client_types import SwarmAPIError, SwarmTransportError


def _retryable(error: Exception) -> bool:
    if isinstance(error, SwarmTransportError):
        return error.retryable
    if isinstance(error, SwarmAPIError):
        return error.status in {408, 429, 500, 502, 503, 504}
    if isinstance(error, HTTPError):
        return error.code in {408, 429, 500, 502, 503, 504}
    return _transport_failure(error).retryable


def _retry_after(error: Exception) -> float | None:
    if not isinstance(error, SwarmAPIError):
        return None
    body = error.body if isinstance(error.body, dict) else {}
    candidates = [(body.get("retry_after_ms"), 0.001)]
    candidates.extend((value, 1.0) for key, value in error.headers.items() if key.lower() == "retry-after")
    for value, scale in candidates:
        if value is None or isinstance(value, bool):
            continue
        try:
            seconds = float(value) * scale
        except (ValueError, TypeError, OverflowError):
            if scale != 1.0 or not isinstance(value, str):
                continue
            try:
                date = parsedate_to_datetime(value)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
                seconds = max(0.0, (date - datetime.now(timezone.utc)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                continue
        if math.isfinite(seconds) and seconds >= 0:
            return min(30.0, max(0.05, seconds))
    return None


def _retry_call(operation: Callable[[], Any], *, client=None, stop_event=None, check=None,
                delay: Callable[[Exception, int], float], max_retries: int = 3, check_result: bool = True):
    timeout = getattr(client, "retry_timeout", None)
    scope = (client.operation(timeout=timeout, stop_event=stop_event, _kind="retry", _check_exit=check_result)
             if callable(getattr(client, "operation", None)) else
             nullcontext(_Lifetime(timeout, kind="retry", stop_event=stop_event)))
    with scope as lifetime:
        for attempt in range(max_retries + 1):
            lifetime.check()
            if check is not None:
                check()
            try:
                result = operation()
            except Exception as error:
                failure = error
            else:
                if check_result:
                    lifetime.check()
                    if check is not None:
                        check()
                return result
            lifetime.check()
            if not _retryable(failure) or attempt == max_retries:
                raise failure
            lifetime.wait(delay(failure, attempt))
    raise AssertionError("unreachable")
