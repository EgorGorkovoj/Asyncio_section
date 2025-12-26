from contextlib import asynccontextmanager
from typing import AsyncIterator

import aiohttp


class RequestPageLoader:
    """
    Асинхронный загрузчик страниц с использованием aiohttp.ClientSession.

    Параметры:
        session (aiohttp.ClientSession): Асинхронная сессия aiohttp,
                                         используемая для выполнения GET-запросов.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    @asynccontextmanager
    async def get_page_object(self, url: str) -> AsyncIterator[aiohttp.ClientResponse]:
        """
        Асинхронный контекстный менеджер для получения объекта ответа страницы.

        Логика:
            1. Выполняет GET-запрос по указанному URL через aiohttp.ClientSession.
            2. Проверяет статус ответа:
                - Если 429, выбрасывает aiohttp.ClientResponseError с пояснением.
                - Иные ошибки HTTP автоматически выбрасываются через response.raise_for_status().
            3. Возвращает объект aiohttp.ClientResponse в контексте.

        Параметры:
            url (str): Ссылка на страницу для загрузки.

        Возвращает:
            AsyncIterator[aiohttp.ClientResponse]: Асинхронный итератор,
                                                   который предоставляет объект ответа страницы.

        Исключения:
            aiohttp.ClientResponseError: при статусе 429 (Too Many Requests).
            aiohttp.ClientError: при других ошибках HTTP.
        """
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
