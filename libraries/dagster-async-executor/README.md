# dagster-async-executor

An executor for [Dagster](https://github.com/dagster-io/dagster) that adds **native `asyncio` support** for ops and assets.

`dagster-async-executor` lets you:

- Run `async def` ops without manually managing event loops or thread pools.
- Mix sync and async ops in the same job.
- Use dynamic / fan-out graphs with async upstream and downstream dependencies.
- Keep the familiar Dagster executor interface, while enabling more scalable, concurrency‑friendly workloads (e.g. I/O‑heavy tasks, service calls, and streaming patterns).

This integration is a community‑maintained port of the original core PR: [dagster-io/dagster#32833](https://github.com/dagster-io/dagster/pull/32833).

---

## Installation

```bash
pip install dagster-async-executor
```

---

## Quickstart

Use the `async_executor` when defining your job and write your ops as `async def`:

```python
import anyio
import dagster as dg
from dagster_async_executor import async_executor

NUM_FANOUTS = 300
SLEEP_SECONDS = 3


@dg.op(out=dg.DynamicOut())
async def create_dynamic_outputs():
    """Creates a dynamic number of outputs."""
    for i in range(NUM_FANOUTS):
        yield dg.DynamicOutput(value=f"item_{i}", mapping_key=f"key_{i}")


@dg.op
async def process_item(context: dg.OpExecutionContext, item: str):
    """Process each item from the fan-out."""
    context.log.info(f"[{context.op_handle}] sleeping...")
    await anyio.sleep(SLEEP_SECONDS)
    context.log.info(f"[{context.op_handle}] completed")
    return item


@dg.op
async def collect_results(context: dg.OpExecutionContext, results: list):
    """Collect all results from the fan-out."""
    context.log.info(f"[{context.op_handle}] collected {len(results)} results")
    return results


@dg.job(executor_def=async_executor)
def simple_fanout_job():
    # no need to use await
    dynamic_items = create_dynamic_outputs()
    processed = dynamic_items.map(process_item)
    collect_results(processed.collect())
```

Run the job as usual (e.g. via Dagit, `dagster job execute`, or your orchestration environment). From the outside, this executor behaves like a standard Dagster executor – but internally it uses async orchestration.

---

## How it works

At a high level, `dagster-async-executor` introduces an `AsyncExecutor` that:

- Reuses Dagster’s **existing execution plan** machinery.
- Runs steps in an **async orchestration loop** backed by an `anyio.TaskGroup`.
- Bridges between **async step execution** and Dagster’s **synchronous executor interface** via a queue‑based sync–async bridge.

### Execution model

Conceptually, execution looks like this:

1. **Plan creation (sync)**  
   The run, plan, and context are created synchronously, just like with `in_process` and other executors.

2. **Async orchestration loop**  
   Once the plan is ready, an async orchestrator:
   - Schedules each ready step as an async task.
   - Uses `dagster_event_sequence_for_step` to obtain an **async sequence of `DagsterEvent`s** for each step.
   - Sends those events through async streams.

3. **Sync–async event bridge**  
   A synchronous wrapper:
   - Starts the async orchestrator inside an `anyio` `BlockingPortal`.
   - Streams `DagsterEvent`s into a `queue.Queue`.
   - Exposes a standard **`Iterator[DagsterEvent]`** to Dagster’s core execution machinery.

From the rest of the system’s perspective, this executor still “looks like” a normal synchronous executor, which keeps:

- Resource initialization behavior consistent.
- Logging and event semantics unchanged.
- Compatibility with existing Dagster entrypoints and tooling.

### Sync + async ops

Per-step behavior:

- Async orchestration drives all steps.
- Each step:
  - Builds a `step_context`.
  - Iterates over `dagster_event_sequence_for_step(step_context)` in an async `for` loop.
  - Sends each `DagsterEvent` back through the async stream → queue → iterator bridge.

Because the core execution semantics are reused, you can mix sync and async ops in the same graph:

- Async upstream → sync downstream
- Sync upstream → async downstream
- Dynamic outputs and mapped steps that interleave async work

---

## Performance

The executor is designed for I/O‑bound and highly concurrent workloads. An initial performance test (`test_async_executor_performance.py::test_async_performance_basic`) shows improved scaling with increased parallelism.

Example results (fan‑out of async ops sleeping for 3 seconds):

| Number of ops | Sleep (seconds) | Job duration (seconds) |
|--------------:|----------------:|------------------------:|
|             1 |               3 | 3.25                    |
|             5 |               3 | 3.51                    |
|            20 |               3 | 4.23                    |
|           100 |               3 | 6.86                    |
|           300 |               3 | 14.17                   |

These numbers are illustrative; real‑world performance depends on your environment, I/O characteristics, and concurrency limits.

---

## Testing & behavior guarantees

The test suite focuses on validating behavior across a representative set of job shapes:

- **Basic async jobs**
  - Single async op producing a simple output.
  - Multiple async ops with dependencies and parallelism where possible.

- **Mixed sync/async graphs**
  - Async upstream feeding into sync downstream.
  - Sync upstream feeding into async downstream.
  - Ensuring consistent materializations, events, and success/failure semantics across both kinds of ops.

- **Dynamic / fan‑out graphs**
  - Async producers yielding dynamic outputs.
  - Downstream mapping over dynamic keys.
  - Interleaving async mapped steps and verifying all mapped outputs are awaited and collected correctly.

- **Error handling**
  - Exceptions raised from async ops (including inside dynamic maps).
  - Failures reported on the correct steps.
  - Downstream steps cancelled or skipped according to normal Dagster rules.

---

## Limitations & notes

- This executor has not been tested on Python 3.14 free-threaded mode.
- The executor targets **I/O‑bound** concurrency; CPU‑bound workloads should still be offloaded to processes or threads.
- Cancellation, backpressure, and resource lifetime semantics follow Dagster’s existing execution model, but async nuances may still evolve.
- This is a **community‑maintained** integration; behavior may change more rapidly than core Dagster executors as we iterate.
