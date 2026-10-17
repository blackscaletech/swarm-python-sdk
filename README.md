# Swarm Python SDK

Dependency-free Python 3.11+ client and synchronous agent runtimes for Swarm.
Install the official `swarm-sdk` distribution from PyPI and import `swarm_sdk`.
The SDK is open source under Apache-2.0 and uses Swarm's normal API permissions.

## Quickstart

Install the package from PyPI:

```bash
python -m pip install swarm-sdk==0.1.1
```

Provide an authorized API origin and a least-privilege credential in the environment:

```python
import os
from swarm_sdk import SwarmAPIError, SwarmClient

client = SwarmClient(
    base_url=os.environ["SWARM_BASE_URL"],
    token=os.environ["SWARM_TOKEN"],
)
try:
    spaces = client.list_spaces()
except SwarmAPIError as error:
    print(f"Swarm request failed with status {error.status}")
else:
    print(spaces.items)
```

Use HTTPS. The SDK does not grant permissions, broaden token scope, or bypass
approvals. Do not log tokens or sensitive response content. Mutation retries
must reuse the original operation key and exact command; a timeout does not
prove that an external effect did not occur.

See the [PyPI package](https://pypi.org/project/swarm-sdk/) and the
[Swarm API and SDK guide](https://swarm.services/guide/api). `list_spaces()`
returns one `CollectionPage`; use its `next_cursor` for the next authorized page.

## Local Development

Run from this checkout, not a parent repository. No private source, service,
database, provider or account is needed for the tests. Native transport tests
bind only loopback sockets, so the host must permit local socket access.

```bash
python -B scripts/test-unit.py
```

The unit runner rejects skips, private-source reads, external connections and
DNS while allowing the native loopback fixtures. Standard unittest discovery
also works: `python -B -m unittest discover -s tests -v`.

The packaging runtime must already contain the exact versions in
`scripts/requirements-packaging.txt`. Provision it separately if needed, using
an approved local wheelhouse for offline work. The gate never downloads packages:

```bash
python -B scripts/test-packaged.py --dist-dir dist --evidence .local-receipts/packaged
```

Both output directories must be fresh and inside this checkout. The gate builds
an sdist, builds the wheel from that extracted sdist, verifies archive membership
and RECORD hashes, and installs the wheel into a fresh environment. It runs the
self-contained consumer tests and resolves installed type hints with socket/DNS
access denied and original-source reads rejected. It also confirms that all unit
test files and the public contract are present in the sdist. It is not a live
API, provider, deployment or release acceptance test. Temporary files stay in
`.packaged-work/` and are removed on completion.

`swarm_sdk/client-contract.json` is the reviewed public contract snapshot. Tests
check its schema, all canonical authority field sets and every generated
operation policy without reading server implementation sources. Updating that
snapshot requires a coordinated contract review; do not bypass a mismatch.
The wheel includes the contract and the PEP 561 `py.typed` marker.

## Releases

See `CONTRIBUTING.md`, `SECURITY.md`, and `CHANGELOG.md`. CI only tests and builds.
`publish.yml` is manual-only and requires a matching `v<version>` tag, `publish`
set to `true`, and the tag-restricted `pypi` environment. A maintainer reviews the
source and tag before dispatching a release. The PyPI trusted publisher is bound
to this repository, workflow filename and environment. The `publish` input
defaults to `false`. Trusted Publishing uses job-scoped OIDC, not a saved
API token ([PyPI guidance](https://docs.pypi.org/trusted-publishers/using-a-publisher/)).

## Detailed Reference

### Operation Keys And Response Limits

Operation key policy is included in the versioned public client contract. Fresh queue,
acquisition, heartbeat and lease-transition invocations receive new logical
attempt keys; exact retries retain one key. Durable-content operations deduplicate
the normalized convenience command or exact raw JSON body and query. Explicit
caller keys remain unchanged, and no key is generated when the contract does not
require one. Key policy never grants authority or permits an automatic retry of
an uncertain external effect.

Unknown or newer raw routes require `idempotency_key` or an explicit
`idempotency_policy="fresh-attempt"` / `"durable-content"`; otherwise dispatch is
rejected. A conflicting override cannot change a known operation's strategy.
The bundled generated policy is checked against that snapshot by the local tests.

Buffered success responses default to 64 MiB and errors to 1 MiB. Configure
`max_response_bytes` and `max_error_response_bytes` on `SwarmClient`, or override
them on `request`, `data`, `get`, `post` and `patch`. The default urllib transport
reads incrementally and always closes success/error bodies; limits count encoded
bytes and accommodate the current 8 MiB Core output's JSON representation.
Oversized bodies raise `SwarmResponseLimitError` without echoing their content.
Custom transports can implement `request_bounded` with these keyword limits;
otherwise they own allocation and the SDK checks their returned body size.
Optional `response_byte_limits` supplies endpoint-specific ceilings without
changing these defaults. Each rule has `method`, an absolute `/v1/...` `path`
template and at least one of `max_response_bytes`/`max_error_response_bytes`.
Named placeholders occupy whole segments, for example
`/v1/artifacts/{artifactId}/content`; regexes, wildcards, queries and origins are
not accepted. Matching ignores the configured base-URL prefix and query, and
uses the first matching rule. Precedence is explicit per-request limit, matched
rule, global default. Rules are copied and validated at construction.
Larger histories still require pagination.

## Transport Lifetimes

All timeouts below are positive finite seconds or `None`:

- `timeout` defaults to 30 and bounds individual urllib socket waits, not total
  elapsed time. It applies to JSON I/O and stream connection/header I/O.
- `header_timeout` optionally bounds connection through headers as one phase.
  It stops applying when headers arrive, including for long-lived subscriptions.
- `operation_timeout` optionally bounds one buffered request, including headers,
  incremental body reads and decoding. Raw `request`/`data`/`get`/`post`/`patch`
  can override it and accept a `threading.Event` as `stop_event`.
- `stream_idle_timeout` optionally bounds each wait for stream bytes, not callback
  execution. `stream_timeout` optionally bounds the whole stream, including open,
  reads and callbacks. Both stream helpers accept these overrides and `stop_event`.
  After successful headers the ordinary socket timeout no longer applies to SSE.
  Healthy subscriptions have no total/idle deadline unless explicitly configured.
- `retry_timeout` defaults to 150 and bounds each runtime API retry group across
  attempts and Retry-After/backoff waits; `None` disables that additional budget.
  It does not impose a global deadline on a healthy worker loop or host adapter.

Use `with client.operation(timeout=45, stop_event=stop): ...` to share a single
monotonic budget/cancellation event across convenience methods or caller-managed
retries. Nested scopes cannot extend an outer budget. Context is local to the
calling execution context; it is not implicitly inherited by new worker threads.
Per-request deadlines and the retry-group deadline compose by earliest expiry.
Numeric and HTTP-date Retry-After hints are bounded to 30 seconds, with waits
interrupted by cancellation or the remaining monotonic budget. A raw request
never automatically retries a mutation. Runtime retries retain one explicit key
and exact command per API intent; never retry an ambiguous external side effect
without a server receipt/replay contract. Connected Core heartbeat/claim requests
receive same-key recovery, but the loop does not replay acquired execution or
callbacks after a failure. Exhausted retries propagate instead of creating a new
claim intent to conceal uncertainty.
Connected Core start/renew/execute requests also share cancellation and a finite
per-intent budget, but receive no automatic effect retry. Closing its renewal
worker interrupts native in-flight renewal I/O before joining it.

`SwarmTimeoutError` (`kind`: `socket`, `headers`, `operation`, `stream`,
`stream_idle`, `download_idle` or `retry`), `SwarmConnectionError`, `SwarmTLSError`, `SwarmCancelledError` and the
terminal fallback `SwarmTransportError` are distinct `SwarmAPIError` subclasses.
They expose `code`/`retryable`, status zero, no response body/headers and no raw
transport exception chain or diagnostic text. Socket/idle interruptions and
known connection failures are retryable; cancellation, spent budgets and unknown
exceptions are not.
Certificate verification and SSL configuration/protocol failures are terminal
`SwarmTLSError` failures; they are never made retryable by their `ssl.SSLError`
base class. Known SSL EOF/connection-close interruptions remain connection
failures. No TLS certificate, hostname, URL or underlying diagnostic is exposed.
API rejections retain bounded, sanitized structured bodies,
status and retry headers, including real urllib HTTPError responses. Malformed
error JSON still retains the HTTP status; 401/403 do not become network retries.

The default urllib transport owns and closes every buffered/stream response on
success, error, deadline or cancellation. A scoped watchdog shuts down its socket
to unblock native I/O; it is stopped and joined when response ownership ends.
The opener is reused, but this is not a connection-pool guarantee. Direct users
of `UrllibTransport.stream` own its raw returned response and must close it.

**Synchronous limitations:** Python cannot forcibly interrupt arbitrary custom
transports, injected openers, DNS resolvers or callbacks. Native connected socket
I/O is interruptible, but DNS resolution and parts of connection/TLS setup still
depend on stdlib/OS timeout behavior. Custom five-argument `request` transports
and four-argument `stream` transports remain compatible; they receive a clipped
ordinary timeout, with budget/cancellation checks before and after their blocking
work. Legacy `request_bounded` implementations still receive only their two byte
limit keywords. These checks may report an overrun only after custom work returns;
they are not a hard elapsed-time guarantee. Custom stream readers must cooperate
to interrupt a stalled read. Synchronous application and error callbacks also
must return/cooperate; they are not moved into abandoned background threads.
Cancellation/deadline during a callback is checked before acknowledging its ID,
so committed side effects may replay. Make application updates idempotent and
commit cursor/state together. Cancellation of an ambiguous mutation does not prove
that the server applied no effect; reconcile using the original operation key.

## Verified Artifact Content

`get_verified_artifact_content(artifact_id, expected_sha256=None,
max_artifact_bytes=None, ...)` reads the existing **buffered JSON** content route.
It verifies the returned Artifact ID, strict UTF-8 byte length and SHA-256 before
returning the content mapping. An optional expected digest pins the caller's
previously observed content revision; without it, verification proves only that
the returned body agrees with its returned metadata, not independent authenticity
or freshness. Mismatches raise `SwarmArtifactIntegrityError` without content.

`max_artifact_bytes` caps the decoded text's UTF-8 bytes (defaulting to the client's
global success ceiling); JSON wire bytes have a separate `max_response_bytes`
ceiling with the ordinary per-request/route/global precedence. This helper accepts
`operation_timeout` and `stop_event`; its single scope includes both fetch and
chunked hashing. It avoids a second full encoded-body allocation, but still buffers
the JSON response and decoded text. It is not a streaming/binary download API.
`get_artifact_content` also accepts ordinary
request options and safely encodes the Artifact ID as one path segment.

### Verified Raw Download

```python
with client.download_artifact(artifact_id, expected_sha256=observed_sha256) as download:
    chunk = download.read(65536)
    # download.file is a verified, seekable binary file, positioned at zero initially.
```

`download_artifact` opts into authenticated `application/octet-stream` on the same
content route. Its exported `VerifiedArtifactDownload` provides `file`, `read`,
`close`, context-manager cleanup, and `artifact_id`, `content_id`, `sha256`,
`size_bytes`, `etag` and optional `media_type`. No partial file is returned: ID,
content ID, strong ETag, raw byte length and incremental SHA-256 must all agree.
The internal `SpooledTemporaryFile` spills above 1 MiB and reads at most 65,536
bytes per chunk; failures close it automatically. Close a successful result or
use its context manager. Verification describes the bytes at return; caller
edits do not update its metadata. Without an expected digest, this proves
agreement with the authenticated response, not independent freshness.

The byte ceiling is the minimum of `max_artifact_bytes`, the server's 64 MiB,
the matched route/global wire ceiling, and optional `max_response_bytes`.
`max_error_response_bytes` may lower the configured rejection ceiling. The total
`operation_timeout` defaults to 120 seconds (or a smaller client setting), never
exceeds 120 seconds, and composes with an outer operation. `header_timeout` and
`idle_timeout` bound their phases; defaults use the client settings or 30 seconds.
`stop_event` cancels the transfer, hashing and spooling. Native urllib sockets use
the existing watchdog. Downloads never retry automatically, follow redirects,
send Range/conditional requests, or use signed/storage URLs. They request identity
encoding and reject nonidentity Content-Encoding, even if the Artifact's optional
media type describes compressed file bytes. Bounded JSON rejections retain typed
`SwarmAPIError` status (including 410), with sanitized bodies and generic messages.

Custom transports must implement `stream(method, url, headers, timeout)` returning
an owned response with `status`, `headers`, `read(size)` (or `read1(size)`) and
`close()`. They must not redirect or transparently decompress, must honor the
clipped open timeout and bounded reads, and must cooperate to interrupt a blocked
read. The SDK checks deadlines/cancellation before and after calls and closes
responses; it cannot forcibly stop arbitrary synchronous transport/open/DNS work.
Custom final URLs, when exposed by `geturl()`, must equal the requested URL.

Current scope:

- bearer auth and automatic idempotency keys
- direct `bootstrap_space(...)` helper for one-shot headless space setup
- direct `connect_current_user_client(...)` helper for one-shot connected-agent bootstrap from a human session: resolve or create the caller's default personal space and issue a reusable agent credential
- direct `connect_client(...)` helper for one-time agent authorization inside an existing space
- collection helpers with `next_cursor` and `order`
- task and run helpers for planning and inspecting work
- external agent claim and exact-acquire through the existing connection:
  `claim_next_run(...)` and `acquire_run(...)`
- external lease lifecycle: `start_run(...)`, `renew_run_lease(...)`,
  `complete_run(...)` and `release_run_lease(...)`
- host-side external-agent helpers: `execute_agent_acquisition(...)`,
  `run_agent_once(...)` and `run_agent_loop(...)`
- `launch_run(...)`, `launch_task_run(...)`, `spawn_subrun(...)`, `list_space_agent_participants(...)`
  and `declare_executor_capabilities(...)` for scoped coordination
- `create_run_group(...)`, `create_group_run(...)`, `pause_run_group(...)`,
  `resume_run_group(...)` and `cancel_run_group(...)` for bounded work groups
- advanced Connected Core claim and exact-acquire methods:
  `claim_next_core_run(...)` and `acquire_core_run(...)`
- Connected Core lifecycle methods for heartbeat, start, execute, complete,
  renew, and release
- authorized search helper over `/v1/search`
- public search helper over `/v1/search/public`
- Space Intelligence helpers for grounded Ask Space answers, paginated answer history, decision graphs, promotion gates, projections, and active learning queues
- connected chat memory helpers for resume, Ask Space, and session digest flows scoped to `/v1/chat/*`
- audit-event SSE consumption
- connection and presentation flows
- operator, budget, and approval helpers
- token usage telemetry helpers for provider-reported counts and rollups without storing raw prompts or outputs
- Connected Core acquisition and lease lifecycle
- `run_connected_core_once(...)` and `run_connected_core_loop(...)` helpers for
  `heartbeat -> claim -> start -> renew -> execute`
  Successful `execute` calls atomically persist output and return the terminal
  Run. The runtime uses `complete` only to report a sanitized failure.
- `execute_connected_core_acquisition(...)` for exact Run acquisition through
  `POST /v1/core-runs/{runId}/acquire`
- webhook receiver helpers:
  `compute_webhook_signature(...)`, `verify_webhook_signature(...)`, `parse_webhook_headers(...)`
- operator webhook management helpers:
  `list_webhook_actions(...)`, `list_webhook_action_presets(...)`, `list_webhook_targets(...)`, `create_webhook_target(...)`, `create_webhook_target_from_preset(...)`, `update_webhook_target(...)`, `disable_webhook_target(...)`, `list_webhook_deliveries(...)`, `redrive_webhook_delivery(...)`

Agent queue polls, learning claims, acquisitions and lease transitions use a
fresh default idempotency key for each new attempt. For a retry after an uncertain
response, retain your own `idempotency_key` and submit the identical request.
Durable commands keep stable default keys. An expired or reassigned lease is not
made valid by replaying an old request.

This package is intentionally stdlib-only so Python agents can adopt Swarm without bringing in a new HTTP stack.

## Auth Boundaries

Swarm enforces authority on the backend instead of trusting how the SDK is called.

- human-auth clients can manage entities, spaces, memberships, policies, credentials, and client access packages
- reusable Universal/MCP connections can plan, queue, execute and delegate work
  within their live Space permissions, without enrolling a device runner
- Connected Core credentials are execution-oriented: heartbeat, claim or
  exact acquire, lease lifecycle, execution, and completion
- launching work is a separate Permit-backed Command; runner credentials do
  not create work or administer a Space
- chat sessions stay narrower and should use `/v1/chat/*`

Practical rule:

- use the existing agent connection for normal agent work; never give it a
  human session credential
- use a separate Connected Core client only for the advanced enrolled-runner
  protocol
- do not use Connected Core credentials for Space or credential management
- do not place runner credentials or connection package contents into prompts,
  notes, artifacts, or evaluations

## Contract-Backed Scoring

When a run includes `run["scoring_contract"]` and `run["scoring_contract_hash"]`, the executing agent should publish dimension scores instead of client-authored top-level `score` and `outcome`.

- echo the provided `contract_hash`
- send bounded `dimensions`, optional `confidence`, and optional `signals`
- let the Swarm API derive the stored `score` and `outcome`

Manual or ad hoc runs without a scoring contract can still publish the simpler `outcome` plus `score` evaluation shape.

## Connected Chat Memory

Use these helpers after a chat has exchanged a `SWARM_CONNECT_CODE` for a scoped `swarm_chat_*` token. The active Space is resolved from that chat session, so callers do not pass a `space_id`.

```python
from swarm_sdk import SwarmClient

chat = SwarmClient(
    base_url="https://api.swarm.services",
    token="swarm_chat_...",
)

context = chat.build_connected_chat_context_pack({
    "query": "resume current work",
    "objective": "Return only compact project context.",
    "token_budget": 4000,
})
print(context["token_savings"]["estimated_tokens_avoided"])

answer = chat.ask_connected_chat_space({
    "question": "What decisions did we make last session?",
    "desired_output_shape": "brief_with_rationale",
    "allow_learning_update": False,
})

digest = chat.create_connected_chat_session_digest({
    "summary": "Finished the deployment stability pass.",
    "durable_decisions": ["Use chat-scoped memory endpoints for resume/save/ask."],
    "open_questions": ["Which memory suggestions should be approved?"],
    "next_steps": ["Review memory suggestions in Swarm Services."],
    "create_memory_candidates": True,
})
```

Chat memory helpers can create reviewable memory suggestions when policy allows it. They do not promote memory, manage access, issue credentials, or bypass Space permissions.

## External Agent Example

Use `execution_mode: agent` when work must run in the connected host, not Swarm
Cloud. Create a Task with `create_task(...)`, then queue it with
`launch_task_run(...)`. Assign it to `self`, `specific` executor addresses, `any`
authorized worker or `others` excluding the creator.

For a brief without a saved Task, `launch_run(space_id, goal_summary=...,
run_requirements=...)` queues one Run in a new Group through the same route as
MCP's `swarm_launch_run`. It returns a Run, not a Task/Run wrapper. Use
`run["run_group_id"]` with the Group controls and `list_runs(run_group_id=...)`
to inspect its work. Canceling a Group stops its delegated work too; canceling
a Task only stops pending Runs linked to that Task.

`launch_space_run(...)` and `launch_from_prompt(...)` remain explicit
hosted/device helpers that create a Task and return `{ task, run }`. The former
hosted alias `launch_run(...)` now follows normal coordination. Integrations
that intended hosted execution must use `launch_space_run(...)` instead.
No credential change or device enrollment is needed for external-agent work.

Use a new `idempotency_key` for each new launch or control intent and reuse that
key with the exact body after a lost response. The default key deduplicates
identical requests; it does not distinguish a later, intentional repeat.

Hosted `launch_space_run`, prompt and bootstrap helpers send the exact normalized
command used to derive their keys. Run requirements preserve execution/assignment
modes, target/excluded Identity IDs and all capability/approval fields. Routing
enums are whitespace-trimmed and case-sensitive (`agent`/`core`,
`self`/`specific`/`any`/`others`), as in the canonical Go contract; omitted routing
fields retain legacy server defaults. Identity IDs preserve case; manifest values
are lowercase, sorted and deduplicated. Unknown requirement fields or malformed
lists fail before transport instead of silently widening an instruction.
Context references accept only `resource_type`/`resource_id` for
`run`/`task`/`artifact`/`evaluation`. Scoring contracts preserve case-sensitive
`accepted_evaluation_method_keys` and reject unknown fields/malformed dimension
rules. Server-side authorization and registered-method validation remain
authoritative. Explicit raw requests remain an unnormalized, forward-compatible
escape hatch; their content-derived keys preserve sent string whitespace, null
fields and array order, rather than hashing a different trimmed command.
Only the generic durable default uses the `swarm-sdk-py-v2` namespace and hashes
canonical JSON wire values: object keys sort recursively, array order and exact
string values stay intact, tuples serialize as arrays and JSON object keys follow
the request serializer. Named normalized helper recipes keep their existing
namespaces/normalization, apart from the documented field-coverage fixes. Explicit
caller keys and fresh `swarm-sdk-py-op` attempt keys are unchanged.
These corrected key projections differ from older clients for affected commands.
Retain the original explicit operation key and exact payload when recovering an
outstanding intent across an SDK upgrade. Do not reconstruct an old uncertain
call with a new default key: that could apply it twice.

`update_task(...)` accepts `expected_updated_at`; `launch_task_run(...)` accepts
`expected_task_updated_at`. Use the saved Task's `updated_at` to reject concurrent
changes with `409 task_changed`. Retry a lost response with the original body
and idempotency key, not a newly read timestamp.

This example accepts an execution adapter supplied by your agent host. Swarm
coordinates and publishes the result; it does not launch a model from this SDK.

```python
import os
import threading
from swarm_sdk import SwarmClient, run_agent_loop

worker = SwarmClient(
    base_url=os.environ["SWARM_BASE_URL"],
    token=os.environ["SWARM_CONNECTION_TOKEN"],
)

stop = threading.Event()
completed = 0

def execute(run, context, stop_event):
    # Supply your own host adapter. It must stop when stop_event is set.
    return host_adapter.execute(run, context, stop_event=stop_event)

def on_execution(result):
    global completed
    print(result["run"]["id"], result["run"]["status"])
    completed += 1
    if completed == 3:
        stop.set()  # Runs after completion has been acknowledged, not before.

run_agent_loop(
    worker,
    space_id=os.environ["SWARM_SPACE_ID"],  # One fixed, explicitly chosen Space.
    execute=execute,
    ttl_seconds=300,
    stop_event=stop,
    on_execution=on_execution,
)
```

The adapter returns a mapping with `body`, optional `title` and `media_type`
(default `text/markdown`), and optional `outcome` (default `succeeded`). Success
requires a non-empty body; explicit `failed` or `canceled` results may omit it.
An adapter exception propagates unchanged; the helper best-effort reports only
`outcome: failed`, without publishing exception text as an Artifact.

`run_agent_once(...)` takes the same required `space_id` and `execute` arguments,
plus an optional `cursor`. It returns `idle`, `next_cursor`, `retry_after_ms` and
`acquisition`, retaining cursors on idle pages. Work results also include
`context`, `execution` (the adapter result), `run` (the acknowledged terminal Run)
and `run_lease` (the last active lease snapshot). It returns `None` only when
stopped before claiming. For an explicit `acquire_run(...)` response, use
`execute_agent_acquisition(worker, acquisition, space_id=..., execute=...)`.

The loop follows idle cursors and periodically revisits the queue head without
discarding its tail cursor, guaranteeing a tail request between head revisits
even when each poll is slow. `poll_interval_seconds` defaults to 3 and
`revisit_interval_seconds` to 30; idle `retry_after_ms` hints are bounded to
30 seconds. Optional `on_idle(result)` and `on_execution(result)` callbacks run
on the calling thread. The adapter also runs synchronously there; background
threads only renew/watch the lease, and are stopped and joined on exit. Use a
finite client/adapter transport timeout and cooperative cancellation; custom
transports must honor their timeout/cancellation contract. Python cannot forcibly
stop an arbitrary callback. Cancellation or lost ownership sets
the adapter's stop event, prevents completion, and best-effort releases the lease
even when the caller's stop event is already set. Cleanup release is one request
with its own intent key, without retry backoff, and never replaces the primary
error.

Each API intent has a fresh explicit key reused across at most three transient
retries with bounded backoff, including 429/503 and transport response loss.
401/403, revocation and lease conflicts stop the runtime. Execution and host
callbacks are never retried. Exhausted completion retries propagate uncertainty
and stop the loop; do not restart the helper to recover that same execution.
The helpers do not create Tasks, spawn workers, enroll a Core, issue credentials,
heartbeat a Core or call Cloud execution. A schedule queues work but does not
keep a closed chat running. See the
[agent guide](https://swarm.services/guide/connect/agent.md) for setup and loops.

## Advanced Runner Example

```python
import os
import threading

from swarm_sdk import SwarmClient, run_connected_core_loop

core = SwarmClient(
    base_url=os.environ["SWARM_BASE_URL"],
    token=os.environ["SWARM_CORE_RUNNER_TOKEN"],
)

stop = threading.Event()
run_connected_core_loop(
    core,
    ttl_seconds=60,
    stop_event=stop,
    on_execution=lambda result: print(
        result["run"]["id"],
        result["artifact"]["id"],
    ),
)
```

## Event Streams

Audit and runtime-hint streams use bounded, incremental
[SSE framing](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream).
They preserve field whitespace, inherited/empty event IDs and split UTF-8/line
endings. Unfinished frames at EOF are not delivered; oversized frames are
discarded through their delimiter rather than reparsed from a retained tail.

Default consumption remains permissive: malformed/oversized frames are reported
through `on_stream_error` and skipped. The returned cursor is the last received
complete-frame ID, not proof that application state was updated.

Use `strict=True, last_event_id=saved_cursor` to stop on malformed, oversized,
incomplete or unapplied events. `SwarmStreamError` exposes `code`,
`last_event_id` (last successfully completed application callback) and
`last_received_event_id`. The response closes on success or failure.
Diagnostics carry the same two cursor fields.
Typed transport timeout/connection/cancellation failures also carry these cursor
fields, including when interrupted between frames or during an application
callback. Strict callbacks must finish without cancellation/deadline before the
applied cursor advances; error callback failures never expose their raw chain.

Commit the application update and cursor together in the callback before
returning. Reconnect explicitly with the applied cursor and make repeated
delivery idempotent. Strict parsing does not make the audit feed a complete,
commit-ordered replication log. Treat runtime hints as wakeups and reconcile
current Run, approval and Space state through canonical APIs.

## Learning Claim Completion

The fenced learning-queue API requires the exact `claim_generation` returned by
acquisition. Preserve its decimal-string representation:

```python
item = client.claim_space_learning_queue_item(space_id)
client.complete_space_learning_queue_item(space_id, item["id"], {
    "claim_generation": item["claim_generation"],
})
```

To publish a result instead, pass that same field to
`submit_space_learning_queue_result` with the artifact. Use only one completion
method per attempt. Exact retries recover committed effects; changed content or
an expired/reassigned attempt is refused. Do not look up a new generation to
authorize work performed under an older claim.

This API requires server and worker support for the fenced claim contract;
the SDK cannot supply or bypass that server-side authority.

## Connected Core Outcomes

`on_execution` observes an already confirmed succeeded Run. Callback failure
never submits failed completion, releases that work or retries the observer.
Renewal cleanup cancels owned native I/O and waits at most one second; a custom
synchronous transport that ignores cancellation raises a nonretryable
`SwarmTimeoutError` with kind `lease_cleanup` rather than hanging shutdown.
Such a custom thread cannot be forcibly terminated by the SDK.

Lost start/execute responses, server unavailability and runner-required replies
raise nonretryable `SwarmCoreExecutionUncertainError`, retaining the Run ID,
lease ID and stage. They do not authorize failed completion, release or another
model call. Reconcile through the canonical generation-aware owner. A generic
Run status read is not sufficient evidence for the original attempt. Use the
Core runner's signed-envelope flow when runner execution is required.
