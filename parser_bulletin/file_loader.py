from abc import ABC, abstractmethod
from pathlib import Path

import aiofiles
from aiolimiter import AsyncLimiter
from core.logger import logger
from page_load import RequestPageLoader


class FileDownloader(ABC):
    @abstractmethod
    async def download(self, url: str, file_name: str) -> Path | None:
        pass


class AcyncioXMLFileDownloader(FileDownloader):
    def __init__(self, base_dir: Path, page_loader: RequestPageLoader, limiter: AsyncLimiter):
        self.base_dir = base_dir
        self.page_loader = page_loader
        self.limiter = limiter

    async def download(self, url: str, file_name: str) -> Path | None:
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
