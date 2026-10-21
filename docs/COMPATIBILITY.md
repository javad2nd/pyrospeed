# Compatibility strategy

## Target API family

pyrospeed targets clients that preserve the Pyrogram 2.x API shape and raw MTProto surface. This includes mainstream Pyrogram-family distributions where imports remain `pyrogram`, plus Hydrogram-style clients where equivalent modules are available under `hydrogram`.

## Why there is no version allow-list

Fork distributions often retain the same runtime import namespace while changing package/distribution names. Hard-coding a list of package versions would reject compatible forks and become stale quickly.

Instead, `FrameworkBinding` performs runtime capability discovery:

1. inspect the supplied client's module root;
2. try compatible roots (`pyrogram`, then `hydrogram` when needed);
3. load `.raw` functions/types;
4. locate `file_id.FileId.decode`;
5. locate session/auth primitives only when the accelerated upload path needs them.

## Session acquisition

`SessionFactory` first checks whether the client exposes a `get_session()` whose signature explicitly supports `temporary=True`. If available, that extension is preferred.

Otherwise it uses the Pyrogram-family `Session`/`Auth` classes exposed by the detected framework namespace and creates an owned media session. Cross-DC authorization is exported/imported through the framework raw auth functions.

## Native fallback

Compatibility is deliberately asymmetric:

- small uploads default to native `save_file`;
- unknown-size/in-memory downloads default to native `download_media`;
- CDN/file-reference handling stays inside the client's `get_file` implementation.

This reduces the amount of fork-specific behavior duplicated by pyrospeed.

## Practical support expectation

| Client family | Expected path |
|---|---|
| Pyrogram 2.x | `pyrogram` capability path |
| Kurigram 2.x | `pyrogram` drop-in namespace |
| Pyrofork 2.x | `pyrogram` drop-in namespace |
| Hydrogram | `hydrogram` capability path |
| Other forks | supported when required capabilities preserve compatible signatures |

No library can truthfully guarantee every historical/private fork without executing its exact version. Use `pyrospeed-doctor`, the offline test suite, and one real-account smoke test before production rollout.

## Minimum accelerated capabilities

For the complete facade, the supplied client is expected to expose:

- `save_file`
- `download_media`
- `get_file`
- `storage`
- `rnd_id`

Large upload additionally needs raw upload/auth/session machinery compatible with the detected framework.

## v0.2.1 source/API compatibility

Public facade methods and configuration constructor signatures are unchanged. `large_file_threshold` and `override_client_semaphore` are still accepted; see `README.md` upgrade notes for their corrected behavior. Both inherited and instance-defined stream methods are restored correctly. No application migration is required for the default configuration, but applications depending on changing the Client's semaphore during transfers should explicitly configure the Client instead.

Run the included offline regression suite and a live smoke test for the exact fork/version combination you deploy.
