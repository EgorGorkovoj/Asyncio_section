import datetime
from pathlib import Path

import pandas as pd
from core.logger import logger
from xml_extract.xml_data import MetricTonTableExtractor


class ExcelParseWorker:
    """
    Воркер для парсинга Excel-файлов в отдельном процессе.
    Stateless — безопасен для multiprocessing.
    """

    @staticmethod
    def parse(args: tuple[Path, datetime.date]) -> tuple[pd.DataFrame, datetime.date]:
        try:
            file_path, file_date = args
            extractor = MetricTonTableExtractor(file_path)
            df = extractor.extract()
        except Exception as e:
            logger.warning(f'Ошибка при извлечения данных из Excel {file_path}: {e})')
        return df, file_date
