# Taskforge

## Performance and concurrency

See [Performance and concurrency reference](PERFORMANCE.md) for the reproducible
Milestone 21 workload, uninstrumented local baseline, profiling observations,
contention scope, reproduction commands, and known limitations.

## Operational metric queries

Taskforge emits OpenTelemetry metrics when metrics are enabled. This section is
an operator-facing query guide, not a dashboard or an executable query-language
specification. The examples below are deliberately **conceptual pseudocode**:
translate rate, increase, grouping, maximum, and histogram-quantile operations
into the syntax and exported metric names used by the selected telemetry
backend. Taskforge's OpenTelemetry instrument names are authoritative; an
exporter or backend may translate them.

Production queries should be scoped with the bounded resource or deployment
metadata actually retained by the telemetry pipeline so that environments and
process groups are not mixed. Do not assume a deployment label exists unless it
is present in that pipeline. Counter rates and histogram windows should normally
span several export cycles; five minutes is a useful starting window with
Taskforge's default 60-second export interval, but it is not an alert threshold.

Conceptual operations used below:

- **rate over a window**: per-second change of a counter;
- **increase over a window**: counter delta during the window;
- **sum, grouped by dimensions**: combine additive series across live replicas;
- **maximum fresh observation**: select the largest current gauge observation
  without multiplying a database-derived value reported by multiple replicas;
- **histogram quantile**: combine changes in corresponding histogram buckets,
  preserving the backend's bucket-bound representation and listed dimensions,
  then calculate p50, p95, or p99 from the merged distribution.

Metric dimensions are strictly instrument-specific. The implementation and its
tests in `src/taskforge/metrics.py` and `tests/test_metrics.py` are the schema
authority.

### API traffic

Use `taskforge.api.requests` for request volume and
`taskforge.api.request.duration` for latency. Both support
`http.request.method`, `http.route`, `http.response.status_class`, and
`taskforge.outcome`.

Suggested views:

- request rate, summed and grouped by route template, method, and status class;
- separate 4xx and 5xx request rates;
- unhandled application failures filtered to
  `taskforge.outcome=unhandled_exception`;
- p50, p95, and p99 request duration, either fleet-wide or grouped by route
  template.

Only registered FastAPI route templates and `unmatched` can appear as
`http.route`; raw request paths are never a metric dimension. `/health` and
`/ready` are excluded. A 4xx response is not automatically a service failure:
view it separately from 5xx responses and unhandled exceptions.

Conceptual request-rate recipe:

```text
INPUT: taskforge.api.requests
WINDOW: 5 minutes
OPERATION: per-second counter rate, then sum across replicas
GROUP BY: http.route, http.request.method, http.response.status_class
```

### Dispatch publication and durable outbox

Use these instruments without combining their distinct semantics:

- `taskforge.dispatch.created`: committed dispatch intents;
- `taskforge.dispatch.publications`: physical broker-send outcomes, grouped by
  `taskforge.outcome` (`accepted`, `timeout`, `rejected`, or `unavailable`);
- `taskforge.dispatch.publish.duration`: physical broker publish latency with
  the same outcome dimension;
- `taskforge.dispatch.publication_records`: post-confirm durable-record outcomes
  grouped by `taskforge.outcome`;
- `taskforge.dispatch.outbox.invalid`: invalid durable outbox records;
- `taskforge.dispatch.outbox.duration`: time from durable creation to physical
  publication;
- `taskforge.dispatch.outbox.pending`: bounded pending-backlog observation with
  `taskforge.saturated`;
- `taskforge.dispatch.outbox.oldest.age`: age in seconds of the oldest observed
  unpublished dispatch.

Suggested views are dispatch creation/publication rates, broker-send failures by
outcome, publication-record persistence failures, invalid-record rate, p95
publish latency, p95 outbox residence time, pending depth, and oldest pending
age. A durable-invalid record is not a physical publication attempt, and a
publication-record persistence result is not a broker-send result.

Outbox gauges require special handling:

- pending depth is capped at **10,000**;
- `taskforge.saturated=true` means the real backlog is **at least** the reported
  value, so the observation is a lower bound rather than an exact depth;
- stale observations are omitted; an absent series means unknown, not zero;
- oldest age advances locally only while its underlying observation remains
  fresh, then is omitted;
- multiple publisher replicas may observe the same database backlog, so select
  the **maximum fresh observation**, never the sum.

Conceptual backlog recipe:

```text
INPUTS: taskforge.dispatch.outbox.pending,
        taskforge.dispatch.outbox.oldest.age
OPERATION: maximum fresh observation across reporting publisher replicas
DISPLAY: pending value together with taskforge.saturated; oldest age separately
MISSING DATA: unknown/stale, never zero-fill
```

### Retries

Use `taskforge.retry.transitions`, grouped by `taskforge.outcome`, to compare
newly scheduled retries with `failed_no_policy`, `failed_exhausted`, expected
`already_scheduled`/`not_eligible` results, and invariant or persistence
failures. Use `taskforge.retry.dispatches`, also grouped by outcome, for
`dispatched`, `skipped`, invariant-failure, and persistence-failure activity.

Use histogram quantiles from `taskforge.retry.delay` to inspect scheduled delay
and from `taskforge.retry.due.age` to inspect how late dispatch occurred after
eligibility. Due age is not total task age. Suggested views are transition and
dispatch rates by outcome plus p50/p95 delay and due age.

### Handlers and result submission

Use `taskforge.handler.executions` and `taskforge.handler.duration`, grouped only
by `taskforge.result.kind` and, when present, `taskforge.failure.kind`. Result
kinds are `success`, `retryable_failure`, `permanent_failure`, and
`cancellation`; bounded failure kinds distinguish handler-reported failure,
handler exception, execution timeout, and claim expiry.

Use `taskforge.task.result_submissions`, grouped by `taskforge.outcome`, for
accepted, identical replay, stale, conflict, rejected, invariant-failure, and
persistence-failure submissions. `taskforge.result.kind` and
`taskforge.failure.kind` are attached only to newly authoritative accepted
results where meaningful. Do not interpret stale, conflict, or rejected
submissions as accepted task failures.

Suggested views are handler rate by result kind, failure rate by bounded failure
kind, p50/p95/p99 handler duration, accepted retryable/permanent failure rate,
and non-authoritative submission outcomes shown separately.

### Workers and claims

Use `taskforge.worker.claims`, grouped by `taskforge.outcome`, for acquired,
replayed, rejected, and infrastructure-failure claim activity. Rejected claims
may additionally be grouped by the bounded `taskforge.rejection.reason`; do not
add worker, task, attempt, capability, or route identifiers.

Use `taskforge.worker.heartbeats`, grouped by `taskforge.outcome` and, for
rejections, `taskforge.rejection.reason`, for heartbeat acceptance, rejection,
and persistence-failure rates. Use `taskforge.worker.running.deliveries` for the
current process-local running-delivery activity.

`taskforge.worker.running.deliveries` is not durable fleet state. Summing values
from currently reporting worker processes is a useful live approximation, but
it is not authoritative after an arbitrary process crash.

### Recovery

Use `taskforge.recovery.scan.candidates`, grouped by `taskforge.scan.kind`, for
the number of candidates returned by completed expired-claim and stale-session
scans. This is an event count, not a current backlog gauge.

Use `taskforge.recovery.scan.operations`, grouped by `taskforge.scan.kind` and
`taskforge.outcome`, for completed, invariant-failure, and persistence-failure
scan attempts. Use `taskforge.recovery.operations` and
`taskforge.recovery.duration`, grouped by `taskforge.recovery.kind` and
`taskforge.outcome`, for recovery results and p50/p95 duration.

Keep expected concurrency and revalidation outcomes—such as candidates that are
no longer expired, claims already terminated, attempts no longer latest,
results already accepted, or already-completed recovery—separate from genuine
invariant and persistence failures.

### Dead letters

Use `taskforge.dead_letters.created`, grouped by `taskforge.reason`, for newly
committed dead letters caused by `permanent_failure` or `retry_exhausted`.
Idempotent already-present records do not increment this counter.

Use `taskforge.dead_letters.operations`, grouped by `taskforge.operation` and
`taskforge.outcome`, to inspect acknowledge, resolve, and redrive activity.
There is intentionally no dead-letter backlog gauge. Inspect the authorized API
for current open population; absence of creation events does not mean the
backlog is empty.

### WebSockets

Use `taskforge.websocket.connection.attempts`, grouped by `taskforge.outcome`,
for accepted, policy-rejected, service-unavailable, capacity-rejected, and
resume-rejected handshakes. Use `taskforge.websocket.resume.outcomes`, grouped
by `taskforge.outcome`, for the more detailed not-requested, resumed,
invalid-cursor, cursor-ahead, and snapshot-required protocol results. A resume
rejection may legitimately contribute to both views because one describes
handshake health and the other describes protocol detail.

Use `taskforge.websocket.connections.active` for current process-local active
connections, `taskforge.websocket.connection.duration` for p50/p95 duration by
`taskforge.disconnect.kind`, `taskforge.websocket.disconnections` for
disconnection rate by that same bounded dimension, and
`taskforge.websocket.backpressure` for slow-consumer pressure events.

Active connections are process-lifetime activity, not durable global state.
Their sum across currently reporting API processes is not authoritative after
arbitrary process crashes.

### Histogram interpretation

Taskforge configures explicit OpenTelemetry histogram boundaries in seconds:

- API request duration: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
  and 10;
- broker publish and recovery duration: 0.001, 0.005, 0.01, 0.025, 0.05, 0.1,
  0.25, 0.5, 1, 2.5, 5, and 10;
- handler duration: 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300,
  900, and 3,600;
- outbox duration, retry delay, and retry due age: 0.1, 0.5, 1, 2.5, 5, 10,
  30, 60, 300, 900, 3,600, 21,600, and 86,400;
- WebSocket connection duration: 1, 5, 15, 30, 60, 300, 900, 1,800, 3,600,
  21,600, and 86,400.

Conceptual quantile recipe:

```text
INPUT: one implemented Taskforge histogram instrument
WINDOW: a selected interval spanning multiple metric exports
OPERATION:
  1. compute the change/rate for each bucket represented by the backend;
  2. combine corresponding buckets across replicas while preserving bucket bounds
     and only the approved grouping dimensions for that instrument;
  3. calculate the desired quantile from the merged cumulative distribution.
OUTPUT: p50, p95, or p99 estimate
```

The overflow bucket contains observations above the largest finite boundary.
Quantiles near or inside that bucket have limited precision; do not present them
as exact latency measurements.

### Cardinality and scope limits

Never add metric dimensions containing request or correlation IDs; workflow,
version, run, task, attempt, dispatch, worker, session, dead-letter, trace, or
span IDs; raw paths or URLs; task/workflow names; task types; capability or
handler names; broker routes; exception classes or messages; principals;
cursors; user strings; or payload/input/output/reference values.

The views above are diagnostic query recipes only. Alerts, thresholds, SLOs,
error budgets, dashboards, recording rules, collector/backend deployment,
global worker/dead-letter population metrics, and incident runbooks are outside
this task.
