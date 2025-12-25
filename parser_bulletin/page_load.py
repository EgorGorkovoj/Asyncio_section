from contextlib import asynccontextmanager
from typing import AsyncIterator

import aiohttp


class RequestPageLoader:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    @asynccontextmanager
    async def get_page_object(self, url: str) -> AsyncIterator[aiohttp.ClientResponse]:
        async with self.session.get(url=url) as response:
            if response.status == 429:
                raise aiohttp.ClientResponseError(
                    status=429,
                    request_info=response.request_info,
                    history=response.history,
                    message='429 Too Many Requests',
                )
            response.raise_for_status()
            yield response
