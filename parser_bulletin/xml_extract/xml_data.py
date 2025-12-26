from pathlib import Path

import pandas as pd


class MetricTonTableExtractor:
    """
    Класс для извлечения таблицы «Единица измерения: Метрическая тонна»
    из XLS-файлов биржи.

    Реализует пошаговый парсинг файла:
        - поиск секции с нужной единицей измерения;
        - чтение двухуровневых заголовков таблицы;
        - сбор финальных названий колонок;
        - извлечение блока данных;
        - нормализацию имён колонок;
        - фильтрацию строк по количеству договоров.

    На выходе — очищенный pandas.DataFrame, готовый для дальнейшей обработки.
    """

    def __init__(self, filepath: Path):
        if not filepath.exists():
            raise FileNotFoundError(f'Файл не найден: {filepath}')

        self.filepath = filepath

    def extract(self) -> pd.DataFrame:
        """
        Извлекает и возвращает таблицу с данными в метрических тоннах.

        Процесс включает:
            - чтение Excel-файла;
            - поиск секции «Единица измерения: Метрическая тонна»;
            - формирование названий колонок из двух строк заголовков;
            - извлечение основной таблицы;
            - очистку колонок;
            - фильтрацию строк с количеством договоров > 0.

        Возвращает:
            pd.DataFrame:
                Финальная очищенная таблица.
        """

        df = pd.read_excel(self.filepath, engine='xlrd', header=None)

        section_index = self._find_section(df)

        header1, header2 = self._get_headers(df, section_index)
        columns = self._build_columns(header1, header2)

        table = self._extract_table(df, section_index, columns)
        table = self._normalize_columns(table)

        return self._filter_by_contracts(table)

    def _find_section(self, df: pd.DataFrame) -> int:
        """
        Находит индекс строки, в которой содержится текст
        «Единица измерения: Метрическая тонна».

        Параметры:
            df (pd.DataFrame): исходный Excel-файл.

        Возвращает:
            int: индекс найденной строки.
        """
        for idx, row in df.iterrows():
            if (
                row.astype(str)
                .str.contains('Единица измерения: Метрическая тонна', case=False)
                .any()
            ):
                return idx

        raise ValueError('Секция "Единица измерения: Метрическая тонна" не найдена!')

    def _get_headers(self, df: pd.DataFrame, section_index: int) -> tuple[pd.Series, pd.Series]:
        """
        Извлекает два уровня заголовков, расположенных после найденной секции.

        Параметры:
            df (pd.DataFrame): исходная таблица.
            section_index (int): индекс строки с названием секции.

        Возвращает:
            tuple[pd.Series, pd.Series]:
                первая и вторая строки заголовков.
        """
        header1 = df.iloc[section_index + 1].fillna('')
        header2 = df.iloc[section_index + 2].fillna('')
        return header1, header2

    def _build_columns(self, header1, header2) -> list[str]:
        """
        Формирует финальный список названий колонок из двух уровней заголовков.

        Правила:
            - если оба уровня присутствуют → "h1 (h2)";
            - если заполнен только первый → h1;
            - если заполнен только второй → h2.

        Параметры:
            header1 (pd.Series): первый уровень заголовков.
            header2 (pd.Series): второй уровень заголовков.

        Возвращает:
            list[str]: список имён колонок.
        """
        columns = []
        for h1, h2 in zip(header1, header2):
            h1 = str(h1).strip()
            h2 = str(h2).strip()

            if h1 and h2:
                columns.append(f'{h1} ({h2})')
            elif h1:
                columns.append(h1)
            else:
                columns.append(h2)

        return columns

    def _extract_table(self, df: pd.DataFrame, section_index: int, columns: list) -> pd.DataFrame:
        """
        Извлекает блок данных, расположенный после заголовков.

        Параметры:
            df (pd.DataFrame): исходный DataFrame.
            section_index (int): строка начала секции.
            columns (list[str]): итоговые имена колонок.

        Возвращает:
            pd.DataFrame: основная таблица без пустых строк.
        """
        table = df.iloc[section_index + 3 :].dropna(how='all')
        table.columns = columns
        return table

    def _normalize_columns(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Нормализует названия колонок:
            - заменяет переносы строк;
            - убирает двойные пробелы;
            - обрезает пробелы по краям.

        Возвращает:
            pd.DataFrame: таблица с нормализованными именами колонок.
        """
        table.columns = [
            str(c).replace('\n', ' ').replace('  ', ' ').strip() for c in table.columns
        ]
        return table

    def _filter_by_contracts(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Фильтрует строки по количеству договоров и удаляет итоговые строки.

        Правила:
            - выбирает колонку, содержащую слова «Договор» и «шт»;
            - приводит её к int, удаляя нецифровые символы;
            - оставляет только строки с количеством договоров > 0;
            - исключает строки, содержащие «Итого» по коду инструмента.

        Возвращает:
            pd.DataFrame: отфильтрованная таблица.
        """
        columns = table.columns.tolist()

        contract_col = next(col for col in columns if 'Договор' in col and 'шт' in col.lower())

        table[contract_col] = (
            table[contract_col]
            .astype(str)
            .str.replace(r'\D+', '', regex=True)
            .replace('', '0')
            .astype(int)
        )
        table = table[table[contract_col] > 0]

        if 'Код Инструмента' in table.columns:
            table = table[~table['Код Инструмента'].str.contains('Итого', na=False)]

        return table
