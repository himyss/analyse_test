import pandas as pd
import numpy as np

class FilteredData:
    _calculated_fields = ['relative_timestamp']  # Атрибут класса: список всех вычисляемых полей

    def __init__(self, raw_df: pd.DataFrame = None, user_id: int = None):
        """
        Контейнер для хранения отфильтрованных данных и метаданных эксперимента.

        Содержит:
        - Таблицу данных (DataFrame) с колонками 'timestamp', 'relative_timestamp', 'is_valid' и др.
        - ID пользователя (user_id)
        - Начальное абсолютное время эксперимента (start_timestamp)
        """
        if raw_df is not None:
            self._data = raw_df.copy()
            self._prepare_data()
        else:
            self._data = None
            self._start_timestamp = None

        self._user_id = user_id

    def _prepare_data(self):
        """
        Подготовка новой таблицы данных:
        - Переименование колонок
        - Добавление флага 'is_valid' (если отсутствует)
        - Обнуление вычисляемых полей
        - Вычисление start_timestamp и относительного времени
        - Добавление человекочитаемого времени 'timestamp_readable'
        """
        # Удаление приставки 'data.' у колонок (если есть)
        self._data.columns = [col.replace('data.', '') for col in self._data.columns]

        # Установка флага валидности, если он отсутствует
        if 'is_valid' not in self._data.columns:
            self._data['is_valid'] = True

        # Обнуление вычисляемых полей
        self._reset_calculated_fields()

        # Вычисление относительного времени только для валидных данных
        valid_data = self._data[self._data['is_valid'] == True]

        if not valid_data.empty:
            self._start_timestamp = valid_data['timestamp'].min()
            self._data.loc[self._data['is_valid'] == True, 'relative_timestamp'] = (
                self._data['timestamp'] - self._start_timestamp
            )
        else:
            self._start_timestamp = None

        # Добавление читаемого времени
        if 'timestamp_readable' not in self._data.columns:
            self._data['timestamp_readable'] = pd.to_datetime(self._data['timestamp'], unit='s').dt.strftime("%Y-%m-%d %H:%M:%S")

    def _reset_calculated_fields(self):
        """
        Обнуление всех вычисляемых полей.
        Если поле отсутствует — создаётся с NaN.
        """
        for field in self._calculated_fields:
            self._data[field] = np.nan

    # --- Методы доступа к данным и метаданным ---

    def get_user_id(self) -> int:
        """Получить идентификатор пользователя."""
        return self._user_id

    def set_user_id(self, new_user_id: int):
        """Установить идентификатор пользователя."""
        self._user_id = new_user_id

    def get_data(self) -> pd.DataFrame:
        """Получить таблицу данных."""
        return self._data

    def set_data(self, new_data: pd.DataFrame):
        """Установить новое значение таблицы данных."""
        self._data = new_data.copy()
        self._prepare_data()

    def get_start_timestamp(self) -> float:
        """Получить начальное абсолютное время эксперимента."""
        return self._start_timestamp
