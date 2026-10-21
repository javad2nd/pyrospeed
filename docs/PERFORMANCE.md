# Performance tuning

## What pyrospeed can optimize

The library can reduce idle time caused by one request waiting on network round trips by having multiple valid part/range requests in flight. It can also keep disk operations away from the main asyncio loop.

It cannot bypass Telegram account/DC/file-size/rate policies or make a link exceed its physical bandwidth.

## Recommended benchmark method

Use `examples/benchmark.py` with the **same file, account, DC path, network, proxy/VPN, and destination chat**. Run each profile multiple times and compare medians rather than one sample.

Useful upload profiles:

```text
native
2x1
4x1
4x2
6x2
8x2
```

`4x2` means 4 media sessions and up to 2 in-flight RPCs per session.

Do not assume 8 is faster than 4. Stop increasing parallelism when throughput plateaus, CPU encryption load spikes, latency worsens, or errors/rate limits increase.

## RAM estimate

With 512-KiB upload parts:

```text
queued payload ~= connections * inflight_per_connection * queue_factor * 512 KiB
```

Balanced default is approximately 8 MiB of queued payload. This excludes Python objects, MTProto buffers, sockets, client caches, and OS page cache.

## Download segmentation

`segment_chunks=4` means each `get_file` range asks for up to 4 MiB (four 1-MiB chunks). More/larger segments reduce scheduling overhead but may reduce load balancing on high-latency or uneven links.

Start with:

- `workers=2` on constrained devices/links;
- `workers=4` as a balanced profile;
- `workers=8` only after measurement.

The download `workers` limit is the maximum number of active ranged read coroutines, **not** a guarantee of that many independent sockets. Pyrogram-family clients may also apply a shared `get_file_semaphore`. PyroSpeed no longer replaces that semaphore due to concurrent-transfer safety. To tune that framework limit, use the Client's supported constructor option (such as `max_concurrent_transmissions`) and benchmark.

## Disk mode

`disk_io="thread"` is the safe default for asyncio servers and bots because ordinary filesystem reads/writes can block. `inline` can reduce thread-hop overhead on exceptionally fast/controlled storage, but measure event-loop latency before using it in production.

## Multiple simultaneous files

`max_parallel_files=1` is intentionally conservative. If one upload uses 8 sessions and you allow 4 simultaneous files, the application may create roughly 32 media sessions for uploads alone. Increase file-level concurrency only when this is deliberate.

## Benchmark caveats

Avoid drawing conclusions from:

- different file sizes for native vs accelerated runs;
- cached vs uncached storage paths;
- different VPN/proxy routes;
- one run per profile;
- benchmark runs during changing network load;
- deleting or sending messages concurrently with the timed transfer unless that is part of the workload.

## stream_media acceleration

Added configurable streaming modes:
- `chunked`: fixed size output similar
- `any`: immediate transport chunk delivery

This allows callers to choose between predictable buffers and lowest latency.
