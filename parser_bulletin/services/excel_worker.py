import datetime
from pathlib import Path

import pandas as pd
from xml_extract.xml_data import MetricTonTableExtractor


class ExcelParseWorker:
    """
    Воркер для парсинга Excel-файлов в отдельном процессе.
    Stateless — безопасен для multiprocessing.
    """

    @staticmethod
    def parse(args: tuple[Path, datetime.date]) -> tuple[pd.DataFrame, datetime.date]:
        file_path, file_date = args

        extractor = MetricTonTableExtractor(file_path)
        df = extractor.extract()

        return df, file_date
