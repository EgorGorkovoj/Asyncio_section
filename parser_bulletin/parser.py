import datetime
from abc import ABC, abstractmethod
from typing import Generator, TypeAlias

from bs4 import BeautifulSoup, Tag
from core.logger import logger

Link_generator: TypeAlias = Generator[tuple[str, datetime.date], None, None]


class LinkExtractor(ABC):
    @abstractmethod
    def extract(self, html: str):
        pass


class BulletinLinkExtractor(LinkExtractor):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def extract(self, html: str) -> Link_generator:
        soup = BeautifulSoup(html, 'lxml')

        # может tryexcept надо?
        for link in self._extract_links(soup):
            href = link.get('href')
            if not href:
                continue
            href = href.split('?')[0]  # type: ignore
            if 'upload/reports/oil_xls/oil_xls_' not in href:
                continue
            if not href.endswith('.xls'):
                continue

            file_date = self._extract_time(href)
            if not file_date:
                continue

            full_url = href if href.startswith('http') else f'{self.base_url}{href}'
            yield full_url, file_date

    def _extract_links(self, soup: BeautifulSoup) -> list[Tag]:
        links = soup.find_all('a', attrs={'class': 'accordeon-inner__item-title link xls'})
        return links

    def _extract_time(self, href: str) -> datetime.date | None:
        date_in_href = href.split('oil_xls_')[1][:8]
        try:
            return datetime.datetime.strptime(date_in_href, '%Y%m%d').date()
        except Exception as e:
            logger.info(f'Не получилось получить время: {e}.')
            return None
