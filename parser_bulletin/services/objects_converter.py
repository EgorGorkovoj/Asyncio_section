from decimal import Decimal

import pandas as pd


class SpimexRowConverter:
    """
    Конвертер строки pandas.DataFrame в словарь полей ORM-модели
    SpamixTradingResults.

    Отвечает за:
        - нормализацию числовых значений (volume, total);
        - преобразование NaN, '-', '' в None;
        - извлечение текстовых полей;
        - приведение типов (str, int, Decimal);
        - исключает поле 'date', которое добавляется на уровне импортера.
    """

    @staticmethod
    def convert_to_dict(
        row: pd.Series,
    ) -> dict:
        volume_val = row['Объем Договоров в единицах измерения']
        if pd.isna(volume_val) or volume_val in ('-', ''):
            volume = None
        else:
            volume = int(volume_val)

        total_val = row['Обьем Договоров, руб.']
        if pd.isna(total_val) or not str(total_val).replace('.', '', 1).isdigit():
            total = None
        else:
            total = Decimal(total_val)

        return {
            'exchange_product_id': str(row['Код Инструмента']).strip(),
            'exchange_product_name': str(row['Наименование Инструмента']).strip(),
            'delivery_basis_name': str(row['Базис поставки']).strip(),
            'volume': volume,
            'total': total,
            'count': int(row['Количество Договоров, шт.']),
        }
