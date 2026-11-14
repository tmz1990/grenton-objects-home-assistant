"""
Shared access to the Grenton Gate HTTP listener.

Local patch (not upstream): the Gate HTTP handles about one TCP connection
at a time. With every entity polling on its own connection and on the same
interval, a burst of ~20 simultaneous requests makes the rest wait in SYN
retries and fail with "Connection timeout to host". All entities go through
gate_session(), which sends requests to the Gate one by one, with a short
pause in between, and queues the others (31 objects take ~3 s in total).
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import aiohttp

GATE_MAX_CONCURRENT = 1
GATE_GAP = 0.05  # seconds between one connection closing and the next opening

# sock_connect / sock_read raise ServerTimeoutError (an aiohttp.ClientError),
# which the entity code already catches and logs. A "total" timeout would
# raise a bare TimeoutError and escape those handlers.
GATE_TIMEOUT = aiohttp.ClientTimeout(total=None, sock_connect=5, sock_read=10)

_semaphore: asyncio.Semaphore | None = None


def _gate_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(GATE_MAX_CONCURRENT)
    return _semaphore


@asynccontextmanager
async def gate_session():
    """Drop-in replacement for aiohttp.ClientSession() limited to the Gate's capacity."""
    async with _gate_semaphore():
        try:
            async with aiohttp.ClientSession(timeout=GATE_TIMEOUT) as session:
                yield session
        finally:
            # A connection opened right after the previous one closed is
            # answered only after ~1 s; a short pause keeps it at ~40 ms.
            await asyncio.sleep(GATE_GAP)
