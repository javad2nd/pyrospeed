from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import time
from dataclasses import dataclass
from pathlib import Path

from pyrogram import Client
from pyrospeed import PyroSpeed, TransferConfig, UploadConfig


@dataclass
class Result:
    profile: str
    seconds: float
    mib_s: float


def parse_profile(value: str):
    if value == "native":
        return None
    left, right = value.lower().split("x", 1)
    return int(left), int(right)


async def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=Path)
    parser.add_argument("--chat", default="me")
    parser.add_argument("--profiles", default="native,2x1,4x1,4x2,6x2,8x2")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()

    app = Client(
        "pyrospeed-benchmark",
        api_id=int(os.environ["TG_API_ID"]),
        api_hash=os.environ["TG_API_HASH"],
        session_string=os.environ.get("TG_SESSION_STRING"),
        no_updates=True,
    )

    results: list[Result] = []
    async with app:
        for profile in args.profiles.split(","):
            profile = profile.strip()
            tuning = parse_profile(profile)
            for run_no in range(1, args.runs + 1):
                started = time.perf_counter()
                if tuning is None:
                    message = await app.send_document(args.chat, str(args.file))
                else:
                    c, i = tuning
                    cfg = TransferConfig(upload=UploadConfig(connections=c, inflight_per_connection=i))
                    message = await PyroSpeed(app, cfg).send_document(args.chat, str(args.file))
                elapsed = time.perf_counter() - started
                mib_s = args.file.stat().st_size / (1024 * 1024) / elapsed
                results.append(Result(profile, elapsed, mib_s))
                print(f"{profile} run={run_no}: {elapsed:.3f}s, {mib_s:.2f} MiB/s")
                if args.delete:
                    await message.delete()

    print("\nSummary")
    for profile in dict.fromkeys(r.profile for r in results):
        speeds = [r.mib_s for r in results if r.profile == profile]
        print(f"{profile:>8}: median={statistics.median(speeds):.2f} MiB/s")


if __name__ == "__main__":
    asyncio.run(run())
