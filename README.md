# pyrospeed

`pyrospeed` is an async-first transfer accelerator for Telegram MTProto clients that follow the Pyrogram API shape. It focuses on **single-file upload/download throughput**, bounded memory usage, cancellation safety, and a small integration surface.

The package does **not** replace Pyrogram/Kurigram. You keep your existing `Client`; `pyrospeed` accelerates the file-transfer path.

## Why this design

Telegram's MTProto file documentation explicitly allows/recommends:

- upload chunks up to **512 KiB**;
- multiple in-flight upload calls;
- multiple parallel call queues over separate TCP connections;
- download requests with offsets/limits (Pyrogram exposes these through `Client.get_file()`).

`pyrospeed` applies those primitives while keeping queues bounded.

## Highlights

- Fully asynchronous public API.
- Parallel large-file upload using multiple media sessions.
- Parallel ranged download of one file using independent `get_file()` ranges.
- Bounded upload task window; RAM use is predictable.
- Disk I/O can be moved off the event loop with `asyncio.to_thread` (default).
- Instance-scoped upload patch. It **does not modify `pyrogram.Client` globally**.
- Reference-counted patch context for overlapping sends.
- Native fallback for small uploads, unknown-size downloads, and in-memory downloads.
- Sync or async progress callbacks.
- SOLID-oriented separation: compatibility binding, session factory, uploader, downloader, patch, facade.
- Zero mandatory runtime dependencies beyond the Pyrogram-compatible client you already use.

## Supported implementations

`pyrospeed` uses capability detection rather than a hard-coded fork name.

| Implementation | Import namespace | Expected support |
|---|---|---|
| Pyrogram 2.x | `pyrogram` | Yes |
| Kurigram 2.x | `pyrogram` | Yes |
| Pyrofork 2.x | `pyrogram` | Yes |
| Hydrogram | `hydrogram` | Capability-detected |
| Other Pyrogram forks | usually `pyrogram` | Works when the required raw/session/get_file API is compatible |

Historical releases and private forks can change internals. `pyrospeed` intentionally falls back to native behavior where possible instead of claiming unsafe universal compatibility.

## Installation

From this source tree:

```bash
python -m pip install .
```

Choose **one** Telegram implementation, not several packages that provide the same import namespace:

```bash
# Kurigram
python -m pip install "kurigram>=2" tgcrypto

# or archived upstream Pyrogram
python -m pip install "pyrogram>=2" tgcrypto

# or Pyrofork
python -m pip install "pyrofork>=2" tgcrypto
```

You can also install the matching local extra, e.g. `python -m pip install '.[kurigram]'`.

## 30-second usage

```python
import asyncio
from pyrogram import Client
from pyrospeed import PyroSpeed

app = Client(
    "my_account",
    api_id=12345,
    api_hash="...",
    # Important for concurrent get_file calls used by parallel download.
    max_concurrent_transmissions=8,
)

async def main():
    async with app:
        speed = PyroSpeed(app)

        message = await speed.send_document(
            "me",
            "/data/linux.iso",
            progress=lambda current, total: print(current, total),
        )

        saved_path = await speed.download(
            message,
            file_name="downloads/",
        )
        print(saved_path)

asyncio.run(main())
```

Kurigram and Pyrofork retain the `from pyrogram import Client` import style, so the same code is normally used.

## Accelerate all high-level uploads on one Client

If your application already calls `app.send_document()`, `app.send_video()`, etc., install the instance patch:

```python
from pyrospeed import PyroSpeed

speed = PyroSpeed(app)
speed.install()

# These calls now use pyrospeed's large-file save_file path.
await app.send_document("me", "archive.tar.zst")
await app.send_video("me", "movie.mkv")

speed.uninstall()
```

Or scope it:

```python
async with PyroSpeed(app):
    await app.send_document("me", "large.bin")
```

Only that `app` instance is changed. No class-level monkey patch is performed.

## Direct upload

`upload_file()` returns Telegram's raw `InputFile` / `InputFileBig`, matching the object expected by Pyrogram internals:

```python
uploaded = await speed.upload_file("large.bin")
```

This is useful for advanced raw-API code.

## Generic send wrapper

Any async client method that internally calls `save_file()` can be accelerated:

```python
await speed.send("send_photo", "me", "photo.jpg")
await speed.send("send_voice", "me", "voice.ogg")
```

Small files default to the framework's native implementation because setup overhead usually dominates. Advanced users can set `small_file_strategy="parallel"`; pyrospeed then uses `SaveFilePart` and computes the required whole-file MD5 while streaming the parts.

## Parallel download

`download()` accepts:

- a Pyrogram `Message` containing media;
- a media object (`message.document`, `message.video`, ...);
- a file-id string **if `file_size=` is also supplied**.

```python
path = await speed.download(message.document, "downloads/file.zip")

# file-id-only usage
path = await speed.download(
    message.document.file_id,
    "downloads/file.zip",
    file_size=message.document.file_size,
)
```

The downloader splits the file into ranges. Pyrogram's `get_file()` treats `offset` and `limit` in 1-MiB chunk units, so each worker can fetch a distinct range and write it at the correct offset. It verifies each segment before reporting success. It does **not** override the shared Client semaphore: the framework's own transfer cap may limit effective concurrency and the workers do not necessarily imply independent TCP connections.

Unknown-size or in-memory downloads fall back to `Client.download_media()` by default.

## Tuning

```python
from pyrospeed import (
    PyroSpeed,
    TransferConfig,
    UploadConfig,
    DownloadConfig,
)

config = TransferConfig(
    upload=UploadConfig(
        connections=8,
        inflight_per_connection=2,
        queue_factor=2,
        progress_interval=0.2,
        disk_io="thread",
        small_file_strategy="native",
        max_parallel_files=1,
    ),
    download=DownloadConfig(
        workers=8,
        segment_chunks=4,  # 4 MiB per ranged get_file call
        queue_factor=2,
        progress_interval=0.2,
        override_client_semaphore=True,  # retained for API compatibility; now a no-op
        max_parallel_files=1,
    ),
)

speed = PyroSpeed(app, config)
```

Built-in presets:

```python
TransferConfig.conservative()  # 2 upload connections / 2 download workers
TransferConfig.balanced()      # 4 / 4
TransferConfig.aggressive()    # 8 / 8
```

### Upload memory model

Approximate buffered payload memory:

```text
connections × inflight_per_connection × queue_factor × part_size
```

Balanced defaults:

```text
4 × 2 × 2 × 512 KiB ≈ 8 MiB payload buffer
```

Protocol objects, Python tasks, encryption buffers, sockets, and the client itself add overhead.

## Choosing the fastest profile

There is no globally fastest connection count. Throughput depends on:

- account/server-side limits;
- latency to the Telegram DC;
- bandwidth and packet loss;
- proxy/VPN overhead;
- CPU (MTProto encryption);
- storage read/write speed;
- Telegram Premium/non-Premium throttling policies;
- DC and current server load.

Start with balanced and benchmark `2x1`, `4x1`, `4x2`, `6x2`, `8x2`. Stop increasing parallelism when throughput plateaus or latency/error rate rises.

## Progress callbacks

Both sync and async callbacks are supported:

```python
async def progress(current: int, total: int):
    pct = current * 100 / total if total else 0
    print(f"{pct:6.2f}%")

await speed.send_document("me", "file.bin", progress=progress)
```

Callbacks are throttled by `progress_interval`, with a final completion callback.

## Upgrade notes for v0.2.1

Existing `PyroSpeed(app)`, `speed.install()`/`uninstall()`, `send_*()`, `upload_file()` and `download()` calls retain their signatures. `TransferConfig`, `UploadConfig`, and `DownloadConfig` fields also remain accepted. Behavioral fixes:

- `large_file_threshold` remains accepted as the maximum size eligible for native small-upload fallback, but **MTProto `SaveBigFilePart` is now selected only when file size exceeds 10 MiB**, regardless of that setting. Older code that lowered the threshold to force parallel uploads still enables parallel parts but uses `SaveFilePart` for files <= 10 MiB.
- `override_client_semaphore` is deprecated and ignored (no client-wide temporary reassignment). If your speed depended on that reassignment, set the client's supported `max_concurrent_transmissions` at Client construction, if available.
- Directory downloads discard path components in Telegram-provided filenames. Same-destination concurrent downloads each write into a unique staging file; the last successful atomic replacement wins.
- Truncated or overlong ranged downloads now raise `IncompleteDownloadError` rather than returning a potentially corrupted path. Failed upload part acknowledgments are also errors.
- Progress callbacks run sequentially in snapshot order; this can apply backpressure when a callback is slow.
- The optional `MediaConnectionPool` is still not automatically wired into the transfer engines. Its lifecycle cleanup is more robust, but enabling it requires explicit integration.

## Cancellation and failure behavior

- A failed upload request cancels outstanding upload tasks and closes owned temporary media sessions.
- Downloads first write to an exclusive `*.pyrospeed.<random>.part` file, validate every expected byte, and atomically replace the destination after success.
- Failed/cancelled downloads remove their own temporary file when possible; multiple downloads never share a `.part` file.
- The native Pyrogram `FILE_PART_X_MISSING` resend contract is preserved: if the framework calls patched `save_file(..., file_id=..., file_part=N)`, pyrospeed resends that exact part.

## Event-loop behavior

Regular filesystem I/O is not truly non-blocking on all operating systems. The default `disk_io="thread"` moves file reads/writes to `asyncio.to_thread`, so the caller remains async and the main event loop is not held during disk operations. Set `disk_io="inline"` only when you have measured that the extra scheduling overhead is worse for your workload.

## Security

Never commit:

- Telegram session strings;
- API hashes;
- bot tokens;
- `.session` databases.

Examples read secrets from environment variables. `pyrospeed` does not log credentials.

## Diagnostics

```bash
pyrospeed-doctor
```

It reports installed Telegram client packages and runtime import versions.

## Tests

The core tests do not require Telegram credentials or network access:

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
```

## Benchmark

See `examples/benchmark.py`. It can compare several upload profiles against the native implementation using your own Telegram account. Benchmark with a disposable file/message and delete benchmark messages afterward.

## Architecture

```text
PyroSpeed facade
├── FrameworkBinding      # fork/module capability detection
├── SessionFactory        # independent media sessions
├── ParallelUploader      # chunk scheduling + bounded in-flight tasks
├── ParallelDownloader    # ranged get_file + random-access writer
├── ProgressDispatcher    # concurrency-safe progress aggregation
└── InstanceUploadPatch   # reversible per-client integration
```

This separation keeps Telegram/fork compatibility logic out of transfer scheduling and makes individual components testable.

## Known limits

- Telegram itself sets file-size, rate, flood-wait, Premium, and DC policy limits; pyrospeed cannot bypass them.
- Download acceleration depends on a fork exposing Pyrogram-compatible `get_file(file_id, file_size, limit, offset, ...)` semantics.
- CDN/file-reference errors are delegated to the client's own `get_file()` implementation.
- Installing multiple Pyrogram forks simultaneously is unsupported because several distributions provide the same `pyrogram` module.
- Increasing connections indefinitely is counterproductive; tune with real measurements.

## Documentation

- `docs/API.md` — public API and configuration fields
- `docs/DESIGN.md` — architecture, SOLID boundaries, scheduling, cleanup
- `docs/PERFORMANCE.md` — benchmarking and tuning methodology
- `docs/COMPATIBILITY.md` — fork/version capability strategy and fallback rules
- `docs/QUICKSTART_FA.md` — راه‌اندازی سریع فارسی
- `CHANGELOG.md` — release history

## License

MIT for pyrospeed's own code. Pyrogram/Kurigram/Pyrofork and Telegram are separate projects with their own licenses and terms.
