# راه‌اندازی سریع pyrospeed

## نصب

اگر Kurigram دارید:

```bash
python -m pip install pyrospeed-0.1.0-py3-none-any.whl
python -m pip install kurigram tgcrypto
```

اگر Pyrogram یا Pyrofork دارید، فقط همان implementation را نصب کنید. چند فورک که همگی ماژول `pyrogram` را ارائه می‌کنند هم‌زمان نصب نکنید.

## آپلود ساده

```python
from pyrospeed import PyroSpeed

speed = PyroSpeed(app)
message = await speed.send_document("me", "/data/file.zip")
```

## دانلود ساده

```python
path = await speed.download(message, "downloads/")
print(path)
```

برای دانلود موازی، اندازه فایل باید مشخص باشد. اگر Message یا media object بدهید، `file_size` معمولاً از همان object خوانده می‌شود. برای file-id خالی از metadata، اندازه را صریح بدهید:

```python
path = await speed.download(
    file_id,
    "downloads/file.bin",
    file_size=123_456_789,
)
```

## استفاده بدون تغییر کدهای `app.send_*`

```python
speed = PyroSpeed(app)
speed.install()

await app.send_document("me", "large.bin")
await app.send_video("me", "movie.mkv")

speed.uninstall()
```

این patch فقط روی همان instance اعمال می‌شود و `Client` را به‌صورت global تغییر نمی‌دهد.

## تنظیمات پیشنهادی

شروع امن:

```python
from pyrospeed import PyroSpeed, TransferConfig
speed = PyroSpeed(app, TransferConfig.balanced())
```

سرور/اینترنت ضعیف:

```python
TransferConfig.conservative()
```

سرور و لینک سریع، فقط بعد از benchmark:

```python
TransferConfig.aggressive()
```

## شخصی‌سازی

```python
from pyrospeed import TransferConfig, UploadConfig, DownloadConfig

config = TransferConfig(
    upload=UploadConfig(
        connections=6,
        inflight_per_connection=2,
        queue_factor=2,
        max_parallel_files=1,
        disk_io="thread",
    ),
    download=DownloadConfig(
        workers=6,
        segment_chunks=4,
        queue_factor=2,
        max_parallel_files=1,
        disk_io="thread",
    ),
)
```

بیشتر کردن connection همیشه سرعت را بیشتر نمی‌کند. با `examples/benchmark.py` روی همان سرور، همان حساب و همان فایل، چند profile را مقایسه کنید.

## تست قبل از production

```bash
PYTHONPATH=src python -m unittest discover -s tests -v
pyrospeed-doctor
```

سپس یک smoke test واقعی با حساب Telegram خودتان انجام دهید. تست‌های آفلاین correctness، cleanup و scheduler را پوشش می‌دهند، اما rate limit و throughput واقعی وابسته به DC، حساب و شبکه شماست.
