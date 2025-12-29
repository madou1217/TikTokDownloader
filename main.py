#!/usr/bin/env python3

from asyncio import CancelledError
from asyncio import run
from sys import argv

from src.application import TikTokDownloader

WEB_API_MODE = "7"
WEB_API_LAUNCH_ARGS = {"--web-api", "7"}


def resolve_mode(args: list[str]) -> str | None:
    return WEB_API_MODE if any(arg in WEB_API_LAUNCH_ARGS for arg in args[1:]) else None


async def main(mode: str | None = None):
    async with TikTokDownloader() as downloader:
        try:
            await downloader.run(mode=mode)
        except (
                KeyboardInterrupt,
                CancelledError,
        ):
            return


if __name__ == "__main__":
    run(main(resolve_mode(argv)))
