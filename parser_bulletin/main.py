import asyncio
import datetime
import os
import time
from concurrent.futures import ProcessPoolExecutor
from parser import BulletinLinkExtractor

import aiohttp
from aiolimiter import AsyncLimiter
from collector import BulletinCollector
from core.config import DOWNLOADS_DIR, url
from core.db_depends import AsyncSessionLocal
from core.logger import logger
from file_loader import AcyncioXMLFileDownloader
from page_load import RequestPageLoader
from services.excel_worker import ExcelParseWorker
from services.importer_db import SpimexImporterDB


async def main():
    start = time.time()

    start_date = datetime.date(2023, 1, 1)
    end_date = datetime.datetime.now().date()

    page_semaphore = asyncio.Semaphore(7)  # для парсинга страниц
    # file_semaphore = asyncio.Semaphore(1)  # для скачивания файлов
    file_limiter = AsyncLimiter(7, 1)
    async with aiohttp.ClientSession() as http_session:
        response = RequestPageLoader(session=http_session)
        extractor = BulletinLinkExtractor(url.SITE_URL)
        collector = BulletinCollector(url.PAGE_URL, response, extractor, start_date, end_date)
        links = await collector.collect(semaphore=page_semaphore)
        print(
            links,
            f'Количество ссылок: {len(links)}!',
            f'Время парсинга {time.time() - start}',
            sep='\n',
        )
        downloader = AcyncioXMLFileDownloader(DOWNLOADS_DIR, response, file_limiter)
        downloaded_files = []
        try:
            async with asyncio.TaskGroup() as tg:
                tasks = {
                    tg.create_task(
                        downloader.download(link, link.split('reports/oil_xls/')[-1])
                    ): (link, date)
                    for link, date in links
                }

            for task, (link, date) in tasks.items():
                file_path = task.result()
                downloaded_files.append((file_path, date))
        except* Exception as e:
            for error in e.exceptions:
                logger.warning(f'При создании задач и загрузки файлов получена ошибка: {error}!')
        print(
            downloaded_files,
            f'Количество загруженных файлов: {len(downloaded_files)}!',
            f'Время парсинга + загрузки {time.time() - start}',
            sep='\n',
        )

    df_list = []

    try:
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as pool:
            for df, file_date in pool.map(ExcelParseWorker.parse, downloaded_files):
                df_list.append((df, file_date))
    except Exception as e:
        logger.warning(f'Ошибка при парсинге Excel файлов: {e}')

    try:
        async with AsyncSessionLocal() as session:
            importer = SpimexImporterDB(session)
            await importer.save_table_bulk(df_list)
    except Exception as e:
        logger.warning(f'Ошибка при сохранении в базу: {e}')

    print(f'Время работы программы с загрузкой в бд {time.time() - start}')


if __name__ == '__main__':
    asyncio.run(main())
