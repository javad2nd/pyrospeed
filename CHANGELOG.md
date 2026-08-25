# Changelog

## 0.1.0 - 2026-08-15

Initial release.

- Async-first `PyroSpeed` facade.
- Multi-session bounded-memory upload scheduler.
- Parallel `SaveFilePart` option for small files with MD5.
- Exact missing-part resend support.
- Parallel known-size ranged download with random-access writes.
- Atomic temporary-file replacement and failure cleanup.
- Instance-only reversible `save_file` patch.
- Sync/async throttled progress callbacks.
- Pyrogram-family capability detection and media-session factory.
- Conservative/balanced/aggressive presets plus fine-grained configuration.
- Offline unit tests, benchmark, diagnostics CLI, and design/API/performance/compatibility docs.

- Added optional parallel media iterator with ordered concurrent ranged reads.


- Added optional media DC connection pool layer with Pyrogram compatibility focus.
