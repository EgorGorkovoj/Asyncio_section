import asyncio
import datetime
from asyncio.log import logger
from parser import LinkExtractor

from page_load import RequestPageLoader


class BulletinCollector:
    def __init__(
        self,
        page_url: str,
        loader: RequestPageLoader,
        extractor: LinkExtractor,
        start_date: datetime.date,
        end_date: datetime.date,
    ):
        self.page_url = page_url
        self.loader = loader
        self.extractor = extractor
        self.start_date = start_date
        self.end_date = end_date

    async def collect(self, semaphore: asyncio.Semaphore) -> list[tuple[str, datetime.date]]:
        results = []
        page_number = 1
        next_page_url = self.page_url

        while True:
            try:
                async with semaphore:
                    async with self.loader.get_page_object(next_page_url) as response:
                        html = await response.text()
                stop_all = False

                for file_url, file_date in self.extractor.extract(html):
                    if file_date > self.end_date:
                        continue

                    if self._validate_time(file_date):
                        stop_all = True
                        break

                    results.append((file_url, file_date))

                if stop_all:
                    break

                page_number += 1
                next_page_url = self.page_url + f'?page=page-{page_number}'
            except Exception as e:
                logger.warning(f'Произошла ошибка {e} при получении ссылок')
                break
        return results

    def _validate_time(self, file_date) -> bool:
        return file_date < self.start_date
