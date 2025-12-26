from abc import ABC, abstractmethod
from pathlib import Path

import aiofiles
from aiolimiter import AsyncLimiter
from core.logger import logger
from page_load import RequestPageLoader


class FileDownloader(ABC):
    """
    Абстрактный базовый класс для асинхронных загрузчиков файлов.

    Определяет общий интерфейс для классов, которые реализуют метод 'download'.
    """

    @abstractmethod
    async def download(self, url: str, file_name: str) -> Path | None:
        pass


class AcyncioXMLFileDownloader(FileDownloader):
    """
    Асинхронный загрузчик XML-файлов с поддержкой лимитирования количества одновременных запросов.

    Использует:
        - aiofiles для асинхронной записи на диск;
        - aiolimiter.AsyncLimiter для контроля скорости скачивания;
        - RequestPageLoader для асинхронной загрузки страниц.

    Параметры:
        base_dir (Path): Папка, в которую будут сохраняться загруженные файлы.
        page_loader (RequestPageLoader): Асинхронный загрузчик страниц.
        limiter (AsyncLimiter): Лимитер для ограничения количества одновременных скачиваний.
    """

    def __init__(self, base_dir: Path, page_loader: RequestPageLoader, limiter: AsyncLimiter):
        self.base_dir = base_dir
        self.page_loader = page_loader
        self.limiter = limiter

    async def download(self, url: str, file_name: str) -> Path | None:
        """
        Асинхронно скачивает файл по URL и сохраняет его в базовую директорию.

        Логика:
            1. Создаёт папку base_dir, если она не существует.
            2. Проверяет, есть ли уже файл с таким именем, чтобы избежать повторной загрузки.
            3. Ограничивает одновременные запросы через AsyncLimiter.
            4. Загружает контент с помощью page_loader.
            5. Асинхронно записывает файл на диск.

        Параметры:
            url (str): Ссылка на скачиваемый файл.
            file_name (str): Имя файла для сохранения в base_dir.

        Возвращает:
            Path | None: Путь к сохранённому файлу или None, если файл уже существовал.
        """

        self.base_dir.mkdir(parents=True, exist_ok=True)

        file_path = self.base_dir / file_name

        if file_path.exists():
            logger.info(f'Файл {file_name} уже существует!')
            return file_path

        async with self.limiter:
            async with self.page_loader.get_page_object(url=url) as response:
                content = await response.read()
                async with aiofiles.open(file_path, 'wb') as f:
                    await f.write(content)
        return file_path
