import datetime
from abc import ABC, abstractmethod
from typing import Generator, TypeAlias

from bs4 import BeautifulSoup, Tag
from core.logger import logger

Link_generator: TypeAlias = Generator[tuple[str, datetime.date], None, None]


class LinkExtractor(ABC):
    """
    Абстрактный базовый класс для извлечения ссылок из HTML.

    Наследники должны реализовать метод `extract`.
    """

    @abstractmethod
    def extract(self, html: str):
        pass


class BulletinLinkExtractor(LinkExtractor):
    """
    Извлекатель ссылок на xls-файлы биржевых отчетов «Bulletin» из HTML.

    Параметры:
        base_url (str): Базовый URL сайта, к которому будут добавляться относительные ссылки.
    """

    def __init__(self, base_url: str):
        self.base_url = base_url

    def extract(self, html: str) -> Link_generator:
        """
        Извлекает все ссылки на xls-файлы с отчетами из HTML.

        Логика:
            - Парсит HTML через BeautifulSoup.
            - Фильтрует ссылки по классу и пути 'upload/reports/oil_xls/oil_xls_'.
            - Игнорирует ссылки без href или с некорректным форматом даты.
            - Возвращает генератор кортежей (полная ссылка, дата файла).

        Параметры:
            html (str): HTML-код страницы.

        Возвращает:
            Link_generator: Генератор кортежей (URL файла, дата файла).
        """
        soup = BeautifulSoup(html, 'lxml')

        for link in self._extract_links(soup):
            try:
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
            except Exception as e:
                logger.warning(f'Ошибка при обработке ссылки {link}: {e}')
                continue

    def _extract_links(self, soup: BeautifulSoup) -> list[Tag]:
        """
        Извлекает все теги <a> с нужным классом из HTML.

        Параметры:
            soup (BeautifulSoup): Парсенный объект HTML.

        Возвращает:
            list[Tag]: Список тегов <a>, которые соответствуют отчетам xls.
        """
        links = soup.find_all('a', attrs={'class': 'accordeon-inner__item-title link xls'})
        return links

    def _extract_time(self, href: str) -> datetime.date | None:
        """
        Извлекает дату из URL файла.

        Параметры:
            href (str): URL файла.

        Возвращает:
            datetime.date | None: Дата файла, если удалось распарсить; иначе None.
        """

        date_in_href = href.split('oil_xls_')[1][:8]
        try:
            return datetime.datetime.strptime(date_in_href, '%Y%m%d').date()
        except Exception as e:
            logger.info(f'Не получилось получить время: {e}.')
            return None
