import asyncio
import os

from pyrogram import Client
from pyrospeed import PyroSpeed


async def main() -> None:
    app = Client(
        "pyrospeed-example",
        api_id=int(os.environ["TG_API_ID"]),
        api_hash=os.environ["TG_API_HASH"],
        session_string=os.environ.get("TG_SESSION_STRING"),
        no_updates=True,
    )

    async with app:
        speed = PyroSpeed(app)
        message = await speed.send_document(
            "me",
            os.environ["TG_FILE"],
            progress=lambda current, total: print(f"\r{current/total*100:6.2f}%", end=""),
        )
        print(f"\nmessage_id={message.id}")


if __name__ == "__main__":
    asyncio.run(main())
