# Tests

`datalayers_SUITE.erl` is the Common Test suite. It runs in CI against a live
Datalayers instance (`make ct`, see the project README).

`nif_save_bench.erl` is a **manual** measurement harness. It is compiled together
with the suite but never run by CI, because it needs a live server and prints
numbers for a human to read.

## What it measures

`async_execute_prepare/6` is the one NIF whose body does work that scales with the
caller's input: it copies the whole parameter term into an owned environment
(`OwnedEnv::save`) before submitting. That NIF runs on a dirty CPU scheduler, so
the copy occupies a dirty CPU scheduler thread and blocks only the calling
process; the caller's normal scheduler stays free.

The harness reports both sides of that:

- `submit_us` — how long the caller is blocked by the call, copy included;
- `max_gap_us` — the largest interval a heartbeat process observed. On a node with
  a single normal scheduler (`+S 1`) this stays at its roughly 2 ms baseline
  however large the batch is. That is the expected result, not a broken
  measurement: it shows the copy is not running on a normal scheduler. Before the
  copy moved to dirty CPU, this column tracked `submit_us` (about 13 ms for the
  200k-row case below).

## How to run it

Start a Datalayers instance and build the library first (`rebar3 compile`), then:

```bash
export DATALAYERS_TCP_ADDR=172.100.100.10
erl +S 1 -noshell -pa _build/test/lib/datalayers/ebin \
    -run nif_save_bench main -s init stop
```

`+S 1` puts the heartbeat and the caller on one normal scheduler, which is the
arrangement its gap column measures; drop it to run with the normal scheduler
count. The harness creates its own `nif_bench` database and table.

## How to read the output

```
  value   rows    terms  submit_us   us/row        max_gap_us
   none      0        0         44     0.00              2049
    int   1000    10000         81     0.08              2013
  16KiB  10000   420000       1037     0.10              2045
```

- `value` / `rows` — what the parameter matrix is made of.
- `terms` — `erts_debug:flat_size/1` of the parameter term.
- `submit_us` — duration of the NIF call, i.e. the scheduler time the copy costs.
- `us/row` — the same, per row.
- `max_gap_us` — the largest interval the heartbeat observed. The `none` row is
  the baseline (the heartbeat's own ~2 ms jitter); subtract it to read the stall.

Two things to keep in mind when interpreting the numbers:

- the copy scales with the **number of terms**, not with payload bytes: binaries
  larger than 64 bytes are reference counted, so they are shared rather than deep
  copied;
- the parameter to `RecordBatch` conversion is *not* part of this measurement: it
  runs on a bounded blocking worker, after the submit.
