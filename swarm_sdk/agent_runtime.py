"""Single-worker execution on an existing agent connection and the caller's host."""

from __future__ import annotations

import copy
import math
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Callable
from ._client_retry import _retry_after, _retry_call, _retryable
from ._client_types import SwarmCancelledError

from ._client_security import _sanitize_sensitive_text
from .client import SwarmAPIError, SwarmClient
from .connected_core_runtime import compute_connected_core_loop_delay_seconds

_MAX_RETRIES = 3
_MAX_DELAY_SECONDS = 30.0
_MAX_SCAN_PAGES = 32
_WATCH_INTERVAL_SECONDS = 0.05
_Execute = Callable[[dict[str, Any], dict[str, Any], threading.Event], dict[str, Any]]


_Cancelled = SwarmCancelledError


def _intent_key(action: str) -> str:
    return f"swarm-agent-{action}-{uuid.uuid4().hex}"


def _transient(error: Exception) -> bool:
    return _retryable(error)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _bounded_delay(value: Any, *, scale: float = 1.0) -> float | None:
    number = _number(value)
    if number is None or number < 0:
        return None
    return min(_MAX_DELAY_SECONDS, max(0.05, number * scale))


def _retry_delay(error: Exception, attempt: int) -> float:
    delay = _retry_after(error)
    if delay is not None:
        return delay
    return compute_connected_core_loop_delay_seconds(attempt, 0.25)


def _check_stop(stop_event: threading.Event | None) -> None:
    if stop_event is not None and stop_event.is_set():
        raise _Cancelled()


def _call(
    operation: Callable[[], Any],
    *,
    stop_event: threading.Event | None = None,
    check: Callable[[], None] | None = None,
    client: SwarmClient | None = None,
    check_result: bool = True,
) -> Any:
    return _retry_call(operation, client=client, stop_event=stop_event, check=check,
                       delay=_retry_delay, max_retries=_MAX_RETRIES, check_result=check_result)


def _options(space_id: str, ttl_seconds: int, execute: _Execute) -> str:
    if not isinstance(space_id, str) or not space_id.strip():
        raise ValueError("Agent runtime requires an explicit space_id")
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int) or not 1 <= ttl_seconds <= 3600:
        raise ValueError("ttl_seconds must be an integer between 1 and 3600")
    if not callable(execute):
        raise TypeError("Agent runtime requires an execute callback")
    return space_id.strip()


def _run_in_space(run: Any, run_id: str, space_id: str) -> dict[str, Any]:
    if not isinstance(run, dict) or run.get("id") != run_id or run.get("space_id") != space_id:
        raise ValueError("Agent Run must match the acquired Run and explicit space_id")
    return run


def _executable_run(run: Any, run_id: str, space_id: str, lease_id: str) -> dict[str, Any]:
    run = _run_in_space(run, run_id, space_id)
    if run.get("run_lease_id") != lease_id:
        raise RuntimeError("Agent Run no longer belongs to the acquired lease")
    requirements = run.get("run_requirements")
    if not isinstance(requirements, dict) or requirements.get("execution_mode") != "agent":
        raise ValueError("Agent runtime requires execution_mode='agent'")
    if run.get("stop_requested_at"):
        raise RuntimeError("Agent Run is stopping")
    return run


def _lease_deadline(lease: Any, original: dict[str, Any], ttl_seconds: int, began: float) -> float:
    if (
        not isinstance(lease, dict)
        or lease.get("id") != original["id"]
        or lease.get("run_id") != original.get("run_id")
        or lease.get("leased_by_identity_id") != original.get("leased_by_identity_id")
        or lease.get("core_id")
        or lease.get("status") != "active"
    ):
        raise RuntimeError("Agent lease ownership is no longer valid")
    try:
        expires = datetime.fromisoformat(lease["expires_at"].replace("Z", "+00:00"))
        if expires.tzinfo is None:
            raise ValueError("missing timezone")
        remaining = expires.timestamp() - time.time()
    except (KeyError, AttributeError, TypeError, ValueError, OverflowError):
        raise ValueError("Agent lease requires a valid expires_at") from None
    deadline = min(time.monotonic() + remaining, began + ttl_seconds) - 0.05
    if deadline <= time.monotonic():
        raise RuntimeError("Agent lease expired")
    return deadline


class _LeaseRenewal:
    def __init__(self, client: SwarmClient, lease: dict[str, Any], ttl_seconds: int,
                 stop_event: threading.Event | None) -> None:
        self.client = client
        self.original = copy.deepcopy(lease)
        self.latest_lease = copy.deepcopy(lease)
        self.ttl_seconds = ttl_seconds
        self.external_stop = stop_event
        self.work_stop = threading.Event()
        self._close = threading.Event()
        self._stop_renewing = threading.Event()
        self._lock = threading.Lock()
        self._error: Exception | None = None
        self._deadline = _lease_deadline(lease, lease, ttl_seconds, time.monotonic())
        self._watcher: threading.Thread | None = None
        self._renewer: threading.Thread | None = None

    def _lose(self, error: Exception) -> None:
        with self._lock:
            if self._error is None:
                self._error = error
        self.work_stop.set()

    def check(self) -> None:
        if self.external_stop is not None and self.external_stop.is_set():
            self._lose(_Cancelled())
        with self._lock:
            expired = time.monotonic() >= self._deadline
        if expired:
            self._lose(RuntimeError("Agent lease expired"))
        with self._lock:
            error = self._error
        if error is not None:
            raise error
        _check_stop(self.work_stop)

    def start(self) -> None:
        # Watch independently so cancellation/expiry reaches the adapter even
        # while a renewal request is blocked in the transport.
        self._watcher = threading.Thread(target=self._watch, name="swarm-agent-lease-watch", daemon=True)
        self._watcher.start()

    def _watch(self) -> None:
        while not self._close.wait(_WATCH_INTERVAL_SECONDS):
            try:
                self.check()
            except Exception:
                return

    def renew(self) -> None:
        key = _intent_key("renew")
        began = time.monotonic()
        try:
            lease = _call(
                lambda: self.client.renew_run_lease(self.original["id"], self.ttl_seconds, idempotency_key=key),
                stop_event=self.work_stop, check=self.check, client=self.client,
            )
            self.check()
            deadline = _lease_deadline(lease, self.original, self.ttl_seconds, began)
            with self._lock:
                self.latest_lease = copy.deepcopy(lease)
                self._deadline = deadline
        except Exception as error:
            self._lose(error)
            raise

    def start_renewing(self) -> None:
        self._renewer = threading.Thread(target=self._renew_loop, name="swarm-agent-lease-renewal", daemon=True)
        self._renewer.start()

    def _renew_loop(self) -> None:
        while not self.work_stop.is_set():
            with self._lock:
                interval = max(0.01, (self._deadline - time.monotonic()) * 0.5)
            if self._stop_renewing.wait(interval):
                return
            try:
                self.renew()
            except Exception:
                return

    def stop_renewing(self) -> None:
        self._stop_renewing.set()
        if self._renewer is not None and self._renewer.ident is not None:
            self._renewer.join()

    def close(self) -> None:
        self._close.set()
        self.stop_renewing()
        if self._watcher is not None and self._watcher.ident is not None:
            self._watcher.join()


def _output(result: Any, lease_id: str) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise TypeError("Agent execute callback must return a result mapping")
    outcome = result.get("outcome", "succeeded")
    if outcome not in {"succeeded", "failed", "canceled"}:
        raise ValueError("Agent result outcome must be succeeded, failed, or canceled")
    body = result.get("body", "")
    if not isinstance(body, str) or (outcome == "succeeded" and not body.strip()):
        raise ValueError("Successful agent execution requires a non-empty result body")
    title = result.get("title", "Agent result")
    media_type = result.get("media_type", "text/markdown")
    if not isinstance(title, str) or not isinstance(media_type, str):
        raise TypeError("Agent result title and media_type must be strings")
    if outcome != "succeeded":
        title = _sanitize_sensitive_text(title)
        body = _sanitize_sensitive_text(body)
    return {"lease_id": lease_id, "outcome": outcome, "body": body, "title": title, "media_type": media_type}


def execute_agent_acquisition(
    client: SwarmClient,
    acquisition: dict[str, Any],
    *,
    space_id: str,
    execute: _Execute,
    ttl_seconds: int = 300,
    stop_event: threading.Event | None = None,
) -> dict[str, Any]:
    """Execute once on this host; execute(run, context, stop_event) must cooperate.

    Returns acquisition, context, execution (adapter result), terminal run and
    latest run_lease. Errors propagate; never retry this whole helper after an
    uncertain completion. Only each API request receives bounded same-key retries.
    """
    acquisition = copy.deepcopy(acquisition)
    run = acquisition.get("run") if isinstance(acquisition, dict) else None
    lease = acquisition.get("run_lease") if isinstance(acquisition, dict) else None
    run_id = run.get("id") if isinstance(run, dict) else None
    lease_id = lease.get("id") if isinstance(lease, dict) else None
    if not isinstance(lease_id, str) or not lease_id:
        raise ValueError("Agent acquisition requires run and run_lease identifiers")
    completed = False
    renewal: _LeaseRenewal | None = None

    def complete(payload: dict[str, Any]) -> dict[str, Any]:
        key = _intent_key("complete")
        terminal = _call(lambda: client.complete_run(run_id, payload, idempotency_key=key),
                         stop_event=renewal.work_stop, check=renewal.check, client=client)
        _run_in_space(terminal, run_id, space_id)
        if terminal.get("status") != payload["outcome"]:
            raise RuntimeError("Agent completion did not return the requested terminal Run")
        return terminal

    try:
        space_id = _options(space_id, ttl_seconds, execute)
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("Agent acquisition requires run and run_lease identifiers")
        if acquisition.get("idle", False) is not False:
            raise ValueError("Agent execution requires a non-idle acquisition")
        _executable_run(run, run_id, space_id, lease_id)
        if lease.get("run_id") != run_id or lease.get("core_id"):
            raise ValueError("Agent acquisition requires its own external Run lease")
        renewal = _LeaseRenewal(client, lease, ttl_seconds, stop_event)
        renewal.start()
        key = _intent_key("start")
        started = _call(lambda: client.start_run(run_id, lease_id, idempotency_key=key),
                        stop_event=renewal.work_stop, check=renewal.check, client=client)
        _executable_run(started, run_id, space_id, lease_id)
        if started.get("status") != "running":
            raise RuntimeError("Agent start did not return a running Run")
        renewal.renew()
        renewal.start_renewing()
        context = _call(lambda: client.get_run_context(run_id), stop_event=renewal.work_stop, check=renewal.check, client=client)
        if not isinstance(context, dict):
            raise ValueError("Agent context must include the acquired Run")
        context_run = _executable_run(context.get("run"), run_id, space_id, lease_id)
        if context_run.get("status") != "running":
            raise RuntimeError("Agent context did not return a running Run")
        renewal.check()
        try:
            execution = execute(copy.deepcopy(started), copy.deepcopy(context), renewal.work_stop)
            renewal.check()
            payload = _output(execution, lease_id)
        except Exception as error:
            renewal.stop_renewing()
            if isinstance(error, SwarmAPIError) and error.status in {401, 403, 409}:
                renewal._lose(error)
                raise
            # Only adapter failures are reported as failed, and only while the
            # original lease is still owned. Cleanup cannot replace this error.
            try:
                renewal.check()
                complete({"lease_id": lease_id, "outcome": "failed"})
                completed = True
            except Exception:
                pass
            raise
        renewal.stop_renewing()
        renewal.check()
        terminal = complete(payload)
        completed = True
        return {"acquisition": acquisition, "context": context, "execution": execution,
                "run": terminal, "run_lease": renewal.latest_lease}
    finally:
        if renewal is not None:
            renewal.close()
        if not completed:
            key = _intent_key("release")
            try:
                # Cancellation must not suppress abandonment of the lease.
                client.release_run_lease(lease_id, idempotency_key=key)
            except Exception:
                pass


def _claim(client: SwarmClient, space_id: str, ttl_seconds: int, cursor: str | None,
           stop_event: threading.Event | None) -> dict[str, Any]:
    key = _intent_key("claim")
    try:
        claim = _call(lambda: client.claim_next_run(space_id, ttl_seconds=ttl_seconds, cursor=cursor, idempotency_key=key),
                      stop_event=stop_event, client=client, check_result=False)
    except SwarmAPIError as error:
        _check_stop(stop_event)
        # Compatibility with APIs that return a known no-effect capacity conflict.
        # This does not retry lease ownership or other lifecycle conflicts.
        if error.status != 409 or not isinstance(error.body, dict) or error.body.get("code") != "lease_capacity_reached":
            raise
        return {"idle": True, "retry_after_ms": int(max(2.0, _retry_delay(error, 0)) * 1000),
                **({"next_cursor": cursor} if cursor else {})}
    if not isinstance(claim, dict):
        raise ValueError("Agent claim must return an acquisition or idle page")
    return claim


def run_agent_once(
    client: SwarmClient,
    *,
    space_id: str,
    execute: _Execute,
    ttl_seconds: int = 300,
    cursor: str | None = None,
    stop_event: threading.Event | None = None,
) -> dict[str, Any] | None:
    """Claim one page and execute at most one Run. Idle results retain next_cursor.

    Returns None only if stopped before claiming; otherwise includes acquisition,
    idle, next_cursor, retry_after_ms and, for work, the execution result fields.
    """
    space_id = _options(space_id, ttl_seconds, execute)
    if stop_event is not None and stop_event.is_set():
        return None
    claim = _claim(client, space_id, ttl_seconds, cursor, stop_event)
    if claim.get("idle") is True:
        if claim.get("run") or claim.get("run_lease"):
            if claim.get("run_lease"):
                # Validate/release a malformed acquisition rather than leak it.
                execute_agent_acquisition(client, claim, space_id=space_id, execute=execute,
                                           ttl_seconds=ttl_seconds, stop_event=stop_event)
            raise ValueError("Idle agent claim cannot contain acquired work")
        result = {"acquisition": claim}
    else:
        result = execute_agent_acquisition(client, claim, space_id=space_id, execute=execute,
                                           ttl_seconds=ttl_seconds, stop_event=stop_event)
    result.update(idle=claim.get("idle") is True, next_cursor=claim.get("next_cursor"),
                  retry_after_ms=claim.get("retry_after_ms"))
    return result


def run_agent_loop(
    client: SwarmClient,
    *,
    space_id: str,
    execute: _Execute,
    ttl_seconds: int = 300,
    poll_interval_seconds: float = 3.0,
    revisit_interval_seconds: float = 30.0,
    stop_event: threading.Event | None = None,
    on_execution: Callable[[dict[str, Any]], Any] | None = None,
    on_idle: Callable[[dict[str, Any]], Any] | None = None,
) -> None:
    """One worker in one fixed Space; on_execution runs after result publication.

    Follows idle cursors and revisits the head after the interval or 32 pages.
    No outer execution retry: exhausted requests and callback errors terminate
    the loop. Set stop_event in on_execution to stop after a bounded number.
    """
    space_id = _options(space_id, ttl_seconds, execute)
    poll_delay = _bounded_delay(poll_interval_seconds)
    revisit = _number(revisit_interval_seconds)
    if poll_delay is None or revisit is None or revisit <= 0:
        raise ValueError("Agent loop timing must be finite and positive")
    stopper = stop_event or threading.Event()
    cursor = None
    pages = 0
    last_head = time.monotonic()
    must_scan_tail = False
    while not stopper.is_set():
        resume_cursor = None
        if cursor is not None and not must_scan_tail and (pages >= _MAX_SCAN_PAGES or time.monotonic() - last_head >= revisit):
            resume_cursor = cursor
            cursor = None
        if cursor is None:
            last_head = time.monotonic()
            pages = 0
        try:
            result = run_agent_once(client, space_id=space_id, execute=execute, ttl_seconds=ttl_seconds,
                                    cursor=cursor, stop_event=stopper)
        except _Cancelled:
            if stopper.is_set():
                return
            raise
        if result is None:
            return
        pages += 1
        # A head revisit is one page, not a restart that starves a long tail.
        cursor = resume_cursor or result["next_cursor"] or None
        must_scan_tail = resume_cursor is not None
        if not result["idle"]:
            if on_execution is not None:
                on_execution(result)
            continue
        if on_idle is not None:
            on_idle(result)
        hint = _number(result["retry_after_ms"])
        delay = _bounded_delay(hint, scale=0.001) if hint is not None else None
        if cursor is not None and hint == 0:
            delay = 0.0
        stopper.wait(poll_delay if delay is None else delay)
