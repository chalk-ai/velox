# ConcurrentCounter contention benchmark

`ConcurrentCounterBenchmark.cpp` measures the real `ConcurrentCounter` and
`MallocAllocator`. It includes plain and callback counter updates, 64-byte and
4-KiB allocation/free pairs, batches of 32 live 4-KiB allocations, and a 1-MiB
allocation control that bypasses reservations. Every sample validates its final
counter or allocation balance. Thread creation, joining, and 1,000 warmup
iterations per worker are excluded from timing. Allocator objects have fixed
cache-line alignment so comparison builds use the same shared-atomic placement.

The CSV metric is elapsed wall-clock nanoseconds divided by the total updates or
allocation/free pairs across all workers: inverse throughput, not request latency.
The benchmark prints the number of occupied counter shards to stderr. Ordinary
hash collisions are expected, so the result depends on thread IDs and scheduling.

Build from a Chalk checkout using its Nix/Bazel toolchain and a Velox worktree
with Bazel metadata, such as the `bazel-build` branch:

```bash
nix develop .#ci --command bazel build \
  --override_repository=velox="$counter_checkout" \
  --features=-thin_lto --norun_validations \
  --output_groups=-rules_lint_human,-dwyu \
  @velox//:velox_common_base_benchmarks_ConcurrentCounterBenchmark
```

The CMake target is `velox_concurrent_counter_benchmark`, enabled by
`VELOX_ENABLE_BENCHMARKS`.

To compare revisions, build this same benchmark source against each counter
implementation and copy each resulting executable before rebuilding. The baseline
uses `hash & shardMask_`; the candidate uses
`folly::hash::twang_mix64(hash) & shardMask_`. Keep the compiler flags, dependency
versions, jemalloc, and environment identical. Run the executables sequentially
without other builds or benchmarks running:

```bash
for threads in 1 8 32 96 128; do
  for version in before after; do
    LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
      ./counter-${version} --num_threads="$threads" \
      --iterations=100000 --num_runs=5 \
      > "${version}-${threads}.csv" 2> "${version}-${threads}.stderr"
  done
done
```

Bazel links jemalloc into this executable. The recorded commands also preload
the system jemalloc; the executable's defined `malloc` symbol takes precedence.
For a CMake executable using dynamic malloc, the preload selects jemalloc.

Use `--workload=direct` to isolate the bypass control. Leave `MALLOC_CONF` unset
for the measurements below. ASan/UBSan and TSan binaries are correctness checks,
not performance measurements.

## Correctness coverage

The focused Bazel targets are
`@velox//:velox_common_base_tests_ConcurrentCounterTest` and
`@velox//:velox_common_memory_tests_MallocAllocatorReservationTest`.

- Deterministic aligned synthetic thread hashes with several strides, offsets,
  and non-power-of-two requested shard counts expose weak low bits independently
  of the host standard library. This regression fails with the original mask.
- Single-shard collisions, concurrent reads, negative updates, callback
  rejection, and callback exceptions exercise the locking contract.
- Allocations below, at, and above the reservation threshold survive thread exit
  and cross-thread frees with payload and exact live-byte checks.
- Concurrent failed refills at the allocator cap preserve accounting, then
  recover after the blocking allocation is freed. Exact reservation-boundary
  rejection is repeated to check for leaked credits.

All accounting snapshots in the allocator tests occur after thread joins or
latches; `totalUsedBytes()` is not an atomic snapshot of concurrent mutations.


Run both focused targets with `bazel test`, using the same repository override as
above. Use `--config=asan` for the combined ASan/UBSan configuration. For TSan:

```bash
nix develop .#ci --command bazel test \
  --override_repository=velox="$counter_checkout" \
  --features=-thin_lto --@llvm//config:tsan \
  --custom_malloc=@bazel_tools//tools/cpp:malloc \
  --norun_validations --output_groups=-rules_lint_human,-dwyu \
  --test_env=FOLLY_AVAILABLE_CONCURRENCY_MAX=16 \
  --test_env=TSAN_OPTIONS=symbolize=0:halt_on_error=1 \
  @velox//:velox_common_base_tests_ConcurrentCounterTest \
  @velox//:velox_common_memory_tests_MallocAllocatorReservationTest
```

`TSAN_OPTIONS` avoids a toolchain-internal symbolizer deadlock while leaving race
detection enabled and making every report fatal. The concurrency override bounds
the pre-existing CPU-count-driven test; the adversarial cases still use 16 workers.

## Measurement on 2026-10-01

Linux x86-64, Intel Xeon 6985P-C, 96 logical CPUs (128 workers oversubscribe
the host), Clang/libc++ 22.1.8, `-O2 -DNDEBUG`, ThinLTO disabled, jemalloc
5.3.0 statically linked by Chalk's `--custom_malloc=@jemalloc`, `MALLOC_CONF` unset. Medians of five samples with 100,000
updates or allocation/free pairs per worker. The two executables use the same
Bazel dependency graph and benchmark fixture, differing only in shard selection.

| Workload | Threads | Before (ns/op) | After (ns/op) | Speedup |
|---|---:|---:|---:|---:|
| counter | 1 | 12.91 | 13.45 | 0.96× |
| callback | 1 | 13.66 | 14.89 | 0.92× |
| malloc64 | 1 | 34.59 | 35.08 | 0.99× |
| malloc4096 | 1 | 36.96 | 37.27 | 0.99× |
| batch4096 | 1 | 114.78 | 112.43 | 1.02× |
| direct | 1 | 154.24 | 157.72 | 0.98× |
| counter | 8 | 44.74 | 1.71 | 26.23× |
| callback | 8 | 48.59 | 1.89 | 25.72× |
| malloc64 | 8 | 148.67 | 4.52 | 32.88× |
| malloc4096 | 8 | 153.29 | 4.76 | 32.20× |
| batch4096 | 8 | 225.02 | 14.33 | 15.71× |
| direct | 8 | 81.12 | 85.37 | 0.95× |
| counter | 32 | 46.65 | 5.90 | 7.90× |
| callback | 32 | 50.95 | 6.17 | 8.26× |
| malloc64 | 32 | 134.58 | 14.87 | 9.05× |
| malloc4096 | 32 | 141.83 | 16.02 | 8.85× |
| batch4096 | 32 | 223.19 | 15.95 | 14.00× |
| direct | 32 | 97.63 | 96.77 | 1.01× |
| counter | 96 | 52.79 | 2.55 | 20.68× |
| callback | 96 | 60.52 | 3.21 | 18.83× |
| malloc64 | 96 | 171.50 | 8.34 | 20.56× |
| malloc4096 | 96 | 178.94 | 11.31 | 15.82× |
| batch4096 | 96 | 260.44 | 12.20 | 21.35× |
| direct | 96 | 134.12 | 129.36 | 1.04× |
| counter | 128 | 54.20 | 2.26 | 24.02× |
| callback | 128 | 61.48 | 2.73 | 22.52× |
| malloc64 | 128 | 168.00 | 7.05 | 23.83× |
| malloc4096 | 128 | 178.50 | 8.13 | 21.95× |
| batch4096 | 128 | 250.72 | 8.92 | 28.12× |
| direct | 128 | 124.50 | 131.41 | 0.95× |

The original counter occupies one shard in every multithreaded run. With
128 workers, the mixed hash occupies 76 of 128 shards. The single-thread
cost includes the added hash mixing; the bypass control has no consistent
large change. These are component benchmarks, not an end-to-end replay of
the production query or a measurement of its original container images.

## ThinLTO comparison

Repeating the same fixture with `--features=thin_lto` (the production Bazel
optimization mode), all other settings unchanged:

| Workload | Threads | Before (ns/op) | After (ns/op) | Speedup |
|---|---:|---:|---:|---:|
| counter | 1 | 12.61 | 12.76 | 0.99× |
| callback | 1 | 13.47 | 14.17 | 0.95× |
| malloc64 | 1 | 32.55 | 32.96 | 0.99× |
| malloc4096 | 1 | 35.07 | 34.65 | 1.01× |
| batch4096 | 1 | 104.55 | 99.39 | 1.05× |
| direct | 1 | 133.32 | 133.41 | 1.00× |
| counter | 128 | 55.37 | 2.30 | 24.06× |
| callback | 128 | 63.04 | 2.73 | 23.08× |
| malloc64 | 128 | 167.69 | 6.30 | 26.62× |
| malloc4096 | 128 | 174.52 | 7.09 | 24.61× |
| batch4096 | 128 | 260.93 | 9.22 | 28.30× |
| direct | 128 | 129.89 | 129.44 | 1.00× |

ThinLTO preserves the contention improvement; changing the optimization flag
does not repair the collapsed shard distribution.
