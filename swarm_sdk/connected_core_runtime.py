from __future__ import annotations

import hashlib
import random
import threading
import time
import uuid
from typing import Any, Callable

from .client import SwarmAPIError, SwarmClient
from ._client_retry import _retry_after, _retry_call, _retryable
from ._client_types import SwarmCancelledError, SwarmTimeoutError

_MAX_BACKOFF_SECONDS = 30.0


class SwarmCoreExecutionUncertainError(RuntimeError):
    code = "core_execution_reconciliation_required"
    retryable = False

    def __init__(self, stage: str, run_id: str, lease_id: str):
        super().__init__("Swarm connected Core outcome requires reconciliation; do not complete, release or reexecute this work automatically")
        self.stage, self.run_id, self.lease_id = stage, run_id, lease_id


def _runtime_idempotency_key(action: str, *parts: str) -> str:
    source = ":".join((action, *(str(part).strip() for part in parts)))
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    return f"swarm-core-{action}-{digest[:24]}"


def _poll_retry_delay(error: Exception, attempt: int) -> float:
    hint = _retry_after(error)
    return hint if hint is not None else compute_connected_core_loop_delay_seconds(attempt, 0.25)


def _request_once(client, operation, stop_event):
    return _retry_call(operation, client=client, stop_event=stop_event,
                       delay=_poll_retry_delay, max_retries=0)


def compute_connected_core_loop_delay_seconds(
    failure_count: int,
    base_seconds: float,
    random_value: float | None = None,
) -> float:
    base = max(0.25, float(base_seconds or 3.0))
    exponent = min(max(0, int(failure_count or 0)), 6)
    ceiling = min(_MAX_BACKOFF_SECONDS, base * (2**exponent))
    sampled = random.random() if random_value is None else float(random_value)
    clamped = min(1.0, max(0.0, sampled))
    return ceiling * (0.75 + clamped * 0.25)


class _LeaseRenewal:
    def __init__(
        self,
        client: SwarmClient,
        run_id: str,
        lease_id: str,
        ttl_seconds: int,
        stop_event: threading.Event | None,
    ) -> None:
        self._client = client
        self._run_id = run_id
        self._lease_id = lease_id
        self._ttl_seconds = max(1, int(ttl_seconds))
        self._external_stop = stop_event
        self._stop = threading.Event()
        self.error: Exception | None = None
        self.latest_lease: dict[str, Any] | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="swarm-core-lease-renewal",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)
        if self._thread.is_alive():
            # Custom synchronous transports may ignore cancellation; never hang cleanup.
            raise SwarmTimeoutError("lease_cleanup")

    def _run(self) -> None:
        interval = max(1.0, self._ttl_seconds * 0.5)
        while not self._stop.wait(interval):
            if self._external_stop is not None and self._external_stop.is_set():
                return
            try:
                self.latest_lease = _request_once(self._client, lambda: self._client.renew_core_run_lease(
                    self._run_id,
                    self._lease_id,
                    self._ttl_seconds,
                    idempotency_key=_runtime_idempotency_key(
                        "renew",
                        self._run_id,
                        self._lease_id,
                        str(time.time_ns()),
                    ),
                ), self._stop)
            except Exception as error:
                if isinstance(error, SwarmCancelledError) and self._stop.is_set():
                    return
                self.error = error
                return


def execute_connected_core_acquisition(
    client: SwarmClient,
    acquisition: dict[str, Any],
    *,
    ttl_seconds: int = 60,
    stop_event: threading.Event | None = None,
    on_execution: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    run = acquisition.get("run") if isinstance(acquisition, dict) else None
    lease = acquisition.get("run_lease") if isinstance(acquisition, dict) else None
    run_id = str(run.get("id", "") if isinstance(run, dict) else "").strip()
    lease_id = str(lease.get("id", "") if isinstance(lease, dict) else "").strip()
    if not run_id or not lease_id:
        raise ValueError("Connected Core acquisition requires run and run_lease identifiers")
    completed = False
    renewal: _LeaseRenewal | None = None
    run_lease = lease
    stage = "start"
    try:
        if stop_event is not None and stop_event.is_set():
            raise SwarmCancelledError()
        _request_once(client, lambda: client.start_core_run(
            run_id,
            lease_id,
            idempotency_key=_runtime_idempotency_key("start", run_id, lease_id),
        ), stop_event)
        stage = "renew"
        run_lease = _request_once(client, lambda: client.renew_core_run_lease(
            run_id,
            lease_id,
            ttl_seconds,
            idempotency_key=_runtime_idempotency_key(
                "renew",
                run_id,
                lease_id,
                str(time.time_ns()),
            ),
        ), stop_event)
        renewal = _LeaseRenewal(client, run_id, lease_id, ttl_seconds, stop_event)
        renewal.start()
        stage = "execute"
        execution = _request_once(client, lambda: client.execute_core_run(
            run_id,
            lease_id,
            idempotency_key=_runtime_idempotency_key("execute", run_id, lease_id),
        ), stop_event)
        if isinstance(execution, dict) and execution.get("requires_runner"):
            raise SwarmCoreExecutionUncertainError("runner_required", run_id, lease_id)
        completed_run = execution.get("run") if isinstance(execution, dict) else None
        if (
            not isinstance(completed_run, dict)
            or str(completed_run.get("id", "")).strip() != run_id
            or completed_run.get("status") != "succeeded"
        ):
            raise SwarmCoreExecutionUncertainError(stage, run_id, lease_id)
        completed_run = dict(completed_run)
        # A completed server result is not undone by a local progress/log observer.
        completed = True
        renewal.close()
        run_lease = renewal.latest_lease or run_lease
        if on_execution is not None:
            on_execution(execution)
        return {
            "acquisition": acquisition,
            "execution": execution,
            "run": completed_run,
            "run_lease": run_lease,
        }
    except Exception as error:
        if completed:
            raise
        if renewal is not None:
            renewal.close()
            run_lease = renewal.latest_lease or run_lease
        primary_error = renewal.error if renewal is not None and renewal.error is not None else error
        if isinstance(primary_error, SwarmAPIError) and 400 <= primary_error.status < 500 and primary_error.status not in {408, 429}:
            raise primary_error
        if isinstance(primary_error, SwarmCoreExecutionUncertainError):
            raise primary_error
        raise SwarmCoreExecutionUncertainError(stage, run_id, lease_id) from None
    finally:
        if renewal is not None:
            renewal.close()
        # Lease release can terminalize issued execution; do not guess its outcome.


def run_connected_core_once(
    client: SwarmClient,
    *,
    ttl_seconds: int = 60,
    stop_event: threading.Event | None = None,
    on_execution: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any] | None:
    if stop_event is not None and stop_event.is_set():
        return None
    heartbeat_key = _runtime_idempotency_key("heartbeat", uuid.uuid4().hex)
    _retry_call(lambda: client.record_core_runner_heartbeat(idempotency_key=heartbeat_key),
                client=client, stop_event=stop_event, delay=_poll_retry_delay)
    claim_key = _runtime_idempotency_key("claim", uuid.uuid4().hex)
    acquisition = _retry_call(lambda: client.claim_next_core_run(ttl_seconds, idempotency_key=claim_key),
                              client=client, stop_event=stop_event, delay=_poll_retry_delay, check_result=False)
    if acquisition is None:
        return None
    return execute_connected_core_acquisition(
        client,
        acquisition,
        ttl_seconds=ttl_seconds,
        stop_event=stop_event,
        on_execution=on_execution,
    )


def run_connected_core_loop(
    client: SwarmClient,
    *,
    ttl_seconds: int = 60,
    poll_interval_seconds: float = 3.0,
    stop_event: threading.Event | None = None,
    on_execution: Callable[[dict[str, Any]], Any] | None = None,
    on_idle: Callable[[], Any] | None = None,
) -> None:
    stopper = stop_event or threading.Event()
    while not stopper.is_set():
        try:
            result = run_connected_core_once(
                client,
                ttl_seconds=ttl_seconds,
                stop_event=stopper,
                on_execution=on_execution,
            )
            if result is not None:
                continue
            if on_idle is not None:
                on_idle()
        except SwarmCancelledError:
            if stopper.is_set():
                return
            raise
        # Only heartbeat/claim intents are retried with stable receipts. Never
        # replay an acquired execution or a caller callback after ambiguity.
        stopper.wait(
            compute_connected_core_loop_delay_seconds(
                0,
                poll_interval_seconds,
            )
        )
