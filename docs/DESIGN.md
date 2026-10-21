# Design and SOLID notes

## Goals

1. Increase single-file throughput without unbounded task/RAM growth.
2. Keep public calls async-first and avoid regular filesystem I/O on the event loop by default.
3. Integrate with Pyrogram-family clients without a process-wide monkey patch.
4. Isolate fork/version-sensitive internals from scheduling logic.
5. Preserve native fallback paths where acceleration would be fragile or pointless.

## Components

```text
PyroSpeed
  -> FrameworkBinding
  -> SessionFactory
  -> ParallelUploader
  -> ParallelDownloader
  -> ProgressDispatcher
  -> InstanceUploadPatch
```

### Single-responsibility principle

- `FrameworkBinding`: runtime capability/import resolution only.
- `SessionFactory`: media-session acquisition/ownership only.
- `ParallelUploader`: part scheduling and upload lifecycle only.
- `ParallelDownloader`: ranged reads, random-access writes, download lifecycle only.
- `ProgressDispatcher`: progress accounting/throttling only.
- `InstanceUploadPatch`: reversible integration only.
- `PyroSpeed`: facade/composition only.

### Open/closed and dependency inversion

Uploader/session factory accept injected `binding`, `session_factory`, and native save callable. Tests use fakes rather than a live Telegram account. The transfer algorithms depend on the Pyrogram API *shape* instead of importing a concrete fork inside each algorithm.

### Interface segregation

The compatibility layer requests only the capabilities the accelerated paths need: `save_file`, `download_media`, `get_file`, storage access, raw API types/functions, and random upload ids.

## Upload scheduler

For one transfer, pyrospeed creates a configured number of independent media sessions. Part tasks are distributed round-robin across sessions. Each session has an `asyncio.Semaphore` limiting in-flight RPCs.

The producer never creates tasks for the entire file. It stops at:

```text
connections * inflight_per_connection * queue_factor
```

pending part tasks, waits for at least one task to finish, then continues. This is backpressure: memory is bounded with file size.

On any exception/cancellation, pending tasks are cancelled and all owned sessions are stopped.

## Small-file path

Default is native because setup overhead can dominate small transfers. Advanced users can select `small_file_strategy="parallel"`; pyrospeed then uses `SaveFilePart` and computes the required whole-file MD5 while reading parts.

## Missing-part recovery

Pyrogram-family high-level sends can request `save_file(path, file_id=..., file_part=N)` after a server `FILE_PART_N_MISSING` response. pyrospeed implements that contract and resends exactly the requested byte range using a temporary media session.

## Download scheduler

Known-size downloads are partitioned into independent 1-MiB-based `get_file` ranges. A bounded number of segment tasks run concurrently. Each segment writes directly to its byte offset in a pre-sized temporary file:

- POSIX: `os.pwrite` when available.
- fallback/Windows: lock + `lseek`/`write`.

Regular writes run through `asyncio.to_thread` by default.

The target is only replaced after all segments complete. The descriptor is closed before replacement for Windows compatibility. Failed/cancelled transfers try to remove their unique `.pyrospeed.<random>.part` file. All segment byte counts are checked before the atomic replacement, and the Client semaphore is never monkey-patched.

## Instance-scoped integration

A global `Client.save_file = ...` monkey patch can make unrelated client instances share state and can create hard-to-debug races. `InstanceUploadPatch` assigns only `client.save_file` on one object and restores the exact previous instance state. A per-client patch stack supports multiple independent owners, including out-of-order removals. The stream patch follows the same rules.

## Resource controls

Two different limits exist on purpose:

- *within a file*: connections/workers and in-flight task windows;
- *between files*: `max_parallel_files`.

The default `max_parallel_files=1` prevents an application with many simultaneous `send_*()` calls from multiplying session counts unexpectedly. Raise it only after measuring CPU, memory, sockets, and flood/rate-limit behavior.


## Media DC Pool

Added optional Pyrogram-compatible media connection pooling. It uses session factory hooks and avoids hard dependency on Pyrogram private internals.
