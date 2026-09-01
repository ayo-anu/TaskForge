# Performance and concurrency reference

This document records the bounded local performance and contention evidence produced
for Milestone 21. It is a reproducible development reference, not a production
capacity certification, SLA, SLO, or availability guarantee.

## Provenance

The reference uses two different evidence modes. Keep their results separate:

- **Task 2 uninstrumented local reference baseline:** one discarded warmup followed
  by five complete measured repetitions of the canonical workload. The schema-1
  artifact recorded Git base `dc1e28a898a19b2a0c8db6df2b7f4eae99d72641` with
  `git_dirty=true`; the reviewed measurement implementation was subsequently
  committed as `d59bdf10abdf266aa81d47718c86b443cbd34062`.
- **Task 4 diagnostic profile:** one discarded warmup followed by five complete
  profiled repetitions, plus one bounded WebSocket fan-out probe at each approved
  cardinality. The corrected schema-1 artifact recorded Git base
  `840886f437ab473497fd782bacb8fd38f59da1cd` with `git_dirty=true`; the reviewed
  profiler was subsequently committed as
  `727cfd6ee7bc017c6cf5e94d69c92f69f85164f2`.

The baseline code revision is
`727cfd6ee7bc017c6cf5e94d69c92f69f85164f2`, the repository HEAD immediately
before this Task 5 documentation. The baseline was recorded on 2026-09-01. The
artifacts do not contain a measurement timestamp, so this date identifies the
versioned baseline record rather than an invented artifact field.

All ranges below are the minimum and maximum across five measured repetitions.
Latency ranges contain the extrema of the five repetition-level nearest-rank
percentiles; they are not confidence intervals. Displayed values are rounded to
three decimal places from the artifact values.

## Canonical workload

The workload creates five owners, each with one enabled workflow version containing
four independent root steps and no dependency edges. Each owner starts five runs,
giving 25 active runs and 100 initially runnable task runs.

| Dimension | Canonical value |
|---|---:|
| Owner principals and workflow definitions | 5 |
| Runs per owner | 5 |
| Active workflow runs | 25 |
| Independent runnable roots per run | 4 |
| Runnable task runs | 100 |
| Durable dispatch intents | 100 |
| Accepted broker publications | 100 |
| Workers | 8 |
| Prefetch per worker | 4 |
| Nominal consumer prefetch window | 32 (8 workers × prefetch 4) |
| Authorized WebSocket subscriptions | 25, one per run |
| Connections per owner principal | 5 |
| Deliberately redelivered dispatches | 5 |
| Successful task attempts and authoritative results | 100 |
| Streamed execution events in each profiled repetition | 475 |
| Final durable state | 100 succeeded tasks and 25 succeeded runs |
| Final broker state | 0 ready messages and 0 worker deliveries in flight |

Every fifth run selects its `root-00` message for one deliberate RabbitMQ requeue.
The transport therefore redelivers five messages, while durable claim/result checks
prevent another authoritative handler execution or result. Eight consumers compete
for work. A gate holds handler completion until at least two distinct workers have
entered, and all eight workers executed work in the five Task 2 measured repetitions.

One authorized WebSocket client subscribes to each run from cursor zero before
dispatch. The workload compares the observed ordered stream with durable execution
events and waits for every run to reach `succeeded`.

The separate Task 4 fan-out probe is different: it persists 20 events for one run and
observes 1, 5, and 10 same-run subscribers. It is a structural diagnostic, not a
second five-repetition throughput baseline.

## Recorded environment

| Field | Recorded value |
|---|---|
| Platform | Linux/x86-64 under WSL2 |
| Kernel | `6.6.87.2-microsoft-standard-WSL2` |
| CPU | Intel Core i7-10610U CPU at 1.80 GHz |
| Logical CPUs | 8 |
| Reported host memory total | 8,153,952,256 bytes |
| Python | 3.12.3 |
| PostgreSQL used by the measurements | 16.15 |
| RabbitMQ | 4.3.3 |
| Taskforge | 0.1.0 |
| aio-pika | 9.6.2 |
| asyncpg | 0.31.0 |
| FastAPI | 0.141.1 |
| SQLAlchemy | 2.0.51 |
| Uvicorn | 0.52.1 |
| OpenTelemetry SDK | 1.44.0 |

The measurement PostgreSQL version differs from the PostgreSQL 18.4 image currently
pinned in `compose.yaml`. A run against the Compose image remains a useful behavior
check, but it is not directly comparable to this PostgreSQL 16.15 reference without
recording a new baseline. The artifacts reported no cgroup CPU or memory maximum.

## Task 2 uninstrumented performance baseline

### Throughput

| Measurement | Five-repetition range |
|---|---:|
| Authoritative task throughput | 13.442–16.335 tasks/s |
| Measurement-wall throughput | 10.951–12.929 tasks/s |
| Dispatch creation throughput | 87.364–109.952 dispatches/s |
| Publication throughput | 25.878–33.244 publications/s |

The authoritative denominator begins at the earliest persisted root-task
`runnable → dispatched` event and ends at the latest authoritative result for the
100 canonical tasks. Measurement-wall throughput uses the broader observer interval,
so the two throughput measures are not interchangeable.

### Latency

All values are milliseconds.

| Measurement | p50 range | p95 range | p99 range |
|---|---:|---:|---:|
| Authenticated run-detail request | 221.084–266.242 | 234.454–275.092 | 236.376–286.320 |
| Initial root creation to dispatch | 3,729.045–4,287.345 | 4,168.358–4,602.985 | 4,254.236–4,733.793 |
| Outbox creation to publication | 2,015.297–2,677.766 | 2,079.030–2,745.247 | 2,083.340–2,753.186 |
| Dispatch to claim acquisition | 3,095.701–4,355.084 | 4,627.544–5,625.469 | 4,906.466–5,956.335 |
| Claim acquisition to running | 362.702–474.239 | 591.469–810.111 | 664.515–995.895 |
| Running to authoritative result | 362.822–589.804 | 1,148.424–1,547.137 | 1,294.457–1,774.882 |
| Initial root creation to authoritative result | 8,162.088–9,188.967 | 9,778.073–10,798.302 | 9,866.289–10,977.336 |

The authenticated run-detail p95 was below the project brief's 300 ms development-
hardware target in all five repetitions. This establishes only the result for this
fixed local workload; it is not a production SLO.

### Worker, resource, and PostgreSQL observations

- Fleet claim-to-result occupancy: 65.837%–69.985%.
- Delivery-slot occupancy using 8 workers and prefetch 4: 44.143%–49.046%.
- Process peak resident memory: 137,383,936–144,330,752 bytes.
- Mean sampled host CPU utilization: 23.406%–30.880%.
- Maximum observed active PostgreSQL sessions: 15–17.
- Maximum observed waiting PostgreSQL sessions: 15–17.

Fleet occupancy is the union of authoritative claim-to-result intervals grouped by
worker session. Delivery-slot occupancy divides accumulated claim duration by the
32-slot nominal consumer prefetch window. Neither value is a worker capacity limit.

## Task 4 diagnostic profile

Task 4 attached bounded listeners to the exact workload and API SQLAlchemy engines.
Profiling adds overhead, so Task 4 throughput is intentionally not used as the
uninstrumented local reference baseline.

### Application phase duration

| Complete runner phase | Five-repetition wall-duration range |
|---|---:|
| Dispatch creation | 2.710–3.186 s |
| Broker publication | 4.174–5.050 s |
| Execution completion | 6.885–8.543 s |
| Run reconciliation | 3.440–4.155 s |

These are complete application-operation phase durations. They are not cursor
execution durations or the latency of one database statement.

### PostgreSQL and pool behavior

- The structurally identified workflow-run locking statements on the result path
  totaled 200 cursor executions in every repetition.
- Their cumulative cursor-execution duration was 52.749–62.110 seconds.
- Sampled transaction-ID lock exposure was 47.683–56.104 session-seconds.
- Sampled tuple-lock exposure was 21.110–30.359 session-seconds.
- Both `api` and `workload` pools reached 20 checked-out connections in every
  repetition. Both ended at current occupancy zero with balanced checkout/checkin
  counts and no retained checkout records.
- No access-plan pathology qualified for targeted EXPLAIN follow-up; no targeted
  plans were retained.

Result submission intentionally locks the workflow run while accepting a result and
deriving authoritative progression. The observed serialization protects terminal
state and dependency correctness; lock activity alone does not prove an avoidable
query defect.

Cursor execution is measured only around the SQLAlchemy cursor boundary. Concurrent
cursor durations accumulate and may exceed wall time. Transaction lifetime, complete
application phase duration, PostgreSQL wait samples, and pool occupancy are distinct
measurements.

### Broker publication

Each measured repetition recorded exactly 100 accepted publications during the
publication phase. The summed accepted awaited-publish duration was
1.480–1.730 seconds within a complete broker-publication phase of 4.174–5.050 seconds.
The per-repetition ratio was 34.3%–37.8%.

The publisher currently awaits each broker publication with publisher-confirm
semantics. The measured awaited publish path is a material contributor to the
publication phase. The evidence does not isolate pure confirm-wait cost or establish
that disabling confirms, batching, or concurrent publication is safe or beneficial.
The focused 1/10/100-message attribution probe was not performed.

### WebSocket fan-out

| Subscribers | Persisted events | Durable `list_after` calls | Serializations | Sends | Maximum queue depth | Backpressure events | Slow-consumer terminations | Last cursor |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 20 | 2 | 20 | 20 | 19 | 0 | 0 | 20 |
| 5 | 20 | 2 | 100 | 100 | 19 | 0 | 0 | 20 |
| 10 | 20 | 2 | 200 | 200 | 19 | 0 | 0 | 20 |

Durable reconciliation remained at two reads while serialization and send work
scaled with events × subscribers. The probe queue size was 100.

The corresponding single-probe elapsed/process-CPU observations were:

| Subscribers | Elapsed | Process CPU |
|---:|---:|---:|
| 1 | 0.102317 s | 0.089335 s |
| 5 | 0.016382 s | 0.015064 s |
| 10 | 0.016835 s | 0.015448 s |

These timing values are descriptive only. One probe per cardinality, including its
startup effects, cannot establish a timing trend or CI threshold.

## Contention correctness

`make m21-contention` runs six deterministic real-PostgreSQL scenarios: run creation,
dependency join, claim acquisition, retry scanning with `SKIP LOCKED`, cancellation,
and terminal result/state updates. The suite proves intended blocking relationships,
both cancellation lock orderings, durable uniqueness/idempotency invariants, and
bounded no-deadlock/no-hang behavior. Its elapsed runtime is not a performance
baseline.

## Known limits and interpretation

### Measurement limitations

- This is a local WSL2 development reference, not production capacity certification.
- Hardware, host load, PostgreSQL version, and broker environment affect results.
- Five repetitions give a bounded observed range, not statistical confidence or
  long-duration soak evidence.
- Task 4 instrumentation overhead makes its throughput unsuitable for comparison
  with the uninstrumented Task 2 baseline.
- SQLAlchemy cursor-execution time is not complete query, transaction, phase, or
  application latency. Concurrent cursor totals can exceed wall time.
- PostgreSQL wait session-seconds are cumulative sampled exposure across sessions,
  not elapsed wall time.
- Pool occupancy records checkouts and checkins; it does not measure checkout wait.
- SQLAlchemy instrumentation covers SQLAlchemy-mediated production query activity.
  The direct asyncpg PostgreSQL LISTEN connection is outside that instrumentation.
- WebSocket elapsed and CPU observations are diagnostic, not CI thresholds. The
  fan-out probe ran once at each cardinality.
- No broad EXPLAIN campaign was run because no access-plan pathology qualified for
  focused follow-up.
- None of these measurements is an SLA, SLO, uptime, or enterprise-scale guarantee.

### Observed system characteristics

- Result submission deliberately serializes on workflow-run locking to preserve
  authoritative state transitions under concurrency.
- RabbitMQ delivery is at least once. The controlled redeliveries prove durable
  deduplication for this workload, not exactly-once physical execution or external
  effects.
- The awaited broker publication path is material, but a safer or faster publication
  strategy has not been established.
- Same-run durable WebSocket reconciliation did not increase from 1 to 10 subscribers
  in the bounded probe; serialization and sends naturally increased per subscriber.
- Production's default per-client execution-stream queue is 100 events and is
  configurable from 1 through 1,000. A full queue disconnects that client as a slow
  consumer with WebSocket policy-violation semantics instead of growing without
  bound. It does not affect authoritative workflow execution.
- The observed maximum queue depth of 19 is a result for 20 probe events, not a
  subscriber or event capacity ceiling.

## Reproduction

Install the locked development environment first. PostgreSQL and RabbitMQ must be
reachable. The PostgreSQL value must be an administrative SQLAlchemy URL whose role
may create and drop the harness's safely named temporary databases and create the
required `taskforge_runtime` role. Choose artifact paths outside the repository or
keep generated artifacts untracked.

Canonical workload:

```console
TASKFORGE_RUN_M21_WORKLOAD=1 \
TASKFORGE_M21_DATABASE_URL='<administrative PostgreSQL SQLAlchemy URL>' \
TASKFORGE_M21_AMQP_URL='<RabbitMQ AMQP URL>' \
make m21-workload
```

Uninstrumented measurement:

```console
TASKFORGE_RUN_M21_MEASUREMENT=1 \
TASKFORGE_M21_DATABASE_URL='<administrative PostgreSQL SQLAlchemy URL>' \
TASKFORGE_M21_AMQP_URL='<RabbitMQ AMQP URL>' \
TASKFORGE_M21_MEASUREMENT_OUTPUT='<measurement artifact path>' \
make m21-measurement
```

Contention correctness:

```console
TASKFORGE_RUN_M21_CONTENTION=1 \
TASKFORGE_M21_CONTENTION_DATABASE_URL='<administrative PostgreSQL SQLAlchemy URL>' \
make m21-contention
```

Diagnostic profiling:

```console
TASKFORGE_RUN_M21_PROFILING=1 \
TASKFORGE_M21_DATABASE_URL='<administrative PostgreSQL SQLAlchemy URL>' \
TASKFORGE_M21_AMQP_URL='<RabbitMQ AMQP URL>' \
TASKFORGE_M21_PROFILE_OUTPUT='<profile artifact path>' \
make m21-profiling
```

Never commit populated commands, connection details, credentials, bearer values, or
generated evidence containing them.

## Refreshing this reference

Refresh the baseline when the canonical workload shape, measurement definitions,
measured production paths, major database/broker versions, or execution environment
changes materially. Record the new baseline code revision, date, artifact schema,
environment, and explicit dirty-tree state. Do not silently combine results from
different harness modes or replace a range with a best run.
