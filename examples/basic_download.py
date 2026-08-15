import asyncio
import os

from pyrogram import Client
from pyrospeed import PyroSpeed, TransferConfig


async def main() -> None:
    app = Client(
        "pyrospeed-example",
        api_id=int(os.environ["TG_API_ID"]),
        api_hash=os.environ["TG_API_HASH"],
        session_string=os.environ.get("TG_SESSION_STRING"),
        no_updates=True,
    )

    async with app:
        message = await app.get_messages("me", int(os.environ["TG_MESSAGE_ID"]))
        speed = PyroSpeed(app, TransferConfig.aggressive())
        path = await speed.download(message, "downloads/")
        print(path)


if __name__ == "__main__":
    asyncio.run(main())
