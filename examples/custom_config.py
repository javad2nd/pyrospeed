from pyrospeed import DownloadConfig, TransferConfig, UploadConfig

config = TransferConfig(
    upload=UploadConfig(
        connections=6,
        inflight_per_connection=2,
        queue_factor=2,
        progress_interval=0.10,
    ),
    download=DownloadConfig(
        workers=6,
        segment_chunks=4,
        queue_factor=2,
        progress_interval=0.10,
    ),
)
