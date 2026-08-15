# pyrospeed API reference

## `PyroSpeed(client, config=None)`

High-level facade. The object does not start or stop the Telegram client. Start the client normally, then construct `PyroSpeed`.

### `await speed.send_document(chat_id, document, **kwargs)`

Runs the client's own `send_document()` while an **instance-only** `save_file` patch is active. All keyword arguments are passed through unchanged.

Equivalent wrappers are available for:

- `send_video`
- `send_audio`
- `send_animation`
- generic `send(method_name, *args, **kwargs)`

### `await speed.upload_file(path, ...)`

Low-level upload. Returns a framework raw `InputFile` or `InputFileBig` object.

Parameters mirror the Pyrogram-compatible `save_file` contract:

- `path`: filesystem path or seekable binary file object
- `file_id`: existing upload id used by the framework for a missing-part retry
- `file_part`: exact part to resend
- `progress`: sync or async callback `(current, total, *progress_args)`
- `progress_args`: tuple appended to the callback arguments

### `await speed.download(media, file_name=None, *, file_size=None, in_memory=False, progress=None, progress_args=())`

Parallel download when the byte size is known. `media` may be a Message, a media object, or a file-id string.

A bare file-id string has no size metadata, so pass `file_size=` to enable the parallel path. Unknown-size and in-memory downloads fall back to the client's native `download_media()` by default.

### `speed.install()` / `speed.uninstall()`

Patches only `client.save_file` on the supplied client instance. This lets existing `client.send_*()` calls use pyrospeed without changing every call site.

The patch is reversible and reference-counted for overlapping scoped wrappers.

### async context manager

```python
async with PyroSpeed(app):
    await app.send_document("me", "large.bin")
```

The instance patch is installed on entry and restored on exit.

### `speed.capabilities()`

Returns a diagnostics dictionary containing detected framework namespace/version and active transfer settings.

---

## Configuration

### `UploadConfig`

| Field | Default | Meaning |
|---|---:|---|
| `connections` | 4 | independent owned media sessions per active upload |
| `inflight_per_connection` | 2 | concurrent part RPCs allowed on each media session |
| `queue_factor` | 2 | bounded task-window multiplier |
| `part_size` | 512 KiB | Telegram upload part size; validated against protocol constraints |
| `large_file_threshold` | 10 MiB | boundary for `SaveBigFilePart`; may only be moved earlier, never above 10 MiB |
| `progress_interval` | 0.25 s | callback throttle interval |
| `disk_io` | `thread` | move regular file I/O off the event loop (`thread` or `inline`) |
| `small_file_strategy` | `native` | `native` or optional parallel `SaveFilePart` + MD5 path |
| `max_parallel_files` | 1 | total simultaneous files handled by this uploader object |

Approximate maximum queued payload bytes:

```text
connections * inflight_per_connection * queue_factor * part_size
```

This is not total process memory; protocol/encryption/socket/task overhead is additional.

### `DownloadConfig`

| Field | Default | Meaning |
|---|---:|---|
| `workers` | 4 | concurrent ranged download workers |
| `segment_chunks` | 4 | 1-MiB chunks requested per segment |
| `chunk_size` | 1 MiB | fixed to the framework's `get_file` unit |
| `queue_factor` | 2 | bounded task-window multiplier |
| `progress_interval` | 0.25 s | callback throttle interval |
| `disk_io` | `thread` | move random-access writes off the event loop |
| `override_client_semaphore` | true | temporarily align `get_file_semaphore` to worker count if present |
| `fallback_to_native_for_unknown_size` | true | native download when size is unknown |
| `max_parallel_files` | 1 | simultaneous files handled by this downloader object |

### `TransferConfig`

Groups upload/download settings. Presets:

- `TransferConfig.conservative()`
- `TransferConfig.balanced()`
- `TransferConfig.aggressive()`

Presets are starting points, not speed guarantees.

---

## Exceptions

- `PyroSpeedError`: library base exception
- `CompatibilityError`: required Pyrogram-compatible capabilities could not be found
- `TransferConfigurationError`: invalid protocol/tuning setting
- `UnknownFileSizeError`: parallel download requested without a known size and native fallback disabled
