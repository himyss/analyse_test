"""
Класс для фильтрации сырых данных (RawData) и создания отфильтрованного контейнера (FilteredData).

TODO:
- filter_extreme_relative_timestamp
  (помечать строки с аномально большим или отрицательным relative_timestamp)

- filter_short_gaze_sequence
  (помечать слишком короткие последовательности gaze событий, например < 100 мс)

- filter_negative_timestamp
  (помечать строки с отрицательным timestamp как невалидные)
"""

import time
from datetime import datetime

from raw_data import RawData
from filtered_data import FilteredData

class RawDataFilter:

    DEFAULT_TOLERANCE = 0.02  # Допуск по размеру зрачка для фильтрации стабильности

    @staticmethod
    def create_filtered_data(raw_data: RawData) -> FilteredData:
        """
        Создание объекта FilteredData из RawData.
        """
        return FilteredData(
            raw_df=raw_data.get_raw_data(),
            user_id=raw_data.get_user_id()
        )

    """Методы фильтрации"""

    @staticmethod
    def filter_only_gaze(filtered_data: FilteredData):
        df = filtered_data.get_data()
        mask = df['type'] != 'gaze'
        df.loc[mask, 'is_valid'] = False
        filtered_data.set_data(df)

    @staticmethod
    def filter_invalid_pupils(filtered_data: FilteredData, min_size=1.0, max_size=10.0):
        df = filtered_data.get_data()
        mask = (
            (df['eyeleft.pupildiameter'] < min_size) | (df['eyeleft.pupildiameter'] > max_size) |
            (df['eyeright.pupildiameter'] < min_size) | (df['eyeright.pupildiameter'] > max_size)
        )
        df.loc[mask, 'is_valid'] = False
        filtered_data.set_data(df)

    @staticmethod
    def filter_missing_data(filtered_data: FilteredData):
        df = filtered_data.get_data()
        mask = df[['eyeleft.pupildiameter', 'eyeright.pupildiameter', 'timestamp']].isnull().any(axis=1)
        df.loc[mask, 'is_valid'] = False
        filtered_data.set_data(df)

    @staticmethod
    def filter_by_timestamp(filtered_data: FilteredData, min_timestamp: float = 1577836800):
        df = filtered_data.get_data()
        current_time = time.time()

        if isinstance(min_timestamp, str):
            min_timestamp = datetime.strptime(min_timestamp, "%d/%m/%Y").timestamp()

        mask = (df['timestamp'] < min_timestamp) | (df['timestamp'] > current_time)
        df.loc[mask, 'is_valid'] = False
        filtered_data.set_data(df)

    @staticmethod
    def filter_unrealistic_event_spacing(filtered_data: FilteredData, min_interval: float = 0.005):
        df = filtered_data.get_data()
        mask = (df['timestamp'].diff() < min_interval)
        df.loc[mask, 'is_valid'] = False
        filtered_data.set_data(df)

    @staticmethod
    def filter_constant_pupil(filtered_data: FilteredData, max_static_steps: int = 100, tolerance: float = None):
        """
        Помечает строки, где pupil остается почти постоянным в течение max_static_steps подряд событий.
        """
        df = filtered_data.get_data()

        if tolerance is None:
            tolerance = RawDataFilter.DEFAULT_TOLERANCE

        left_diff = df['eyeleft.pupildiameter'].diff().abs()
        right_diff = df['eyeright.pupildiameter'].diff().abs()

        stable_left = left_diff < tolerance
        stable_right = right_diff < tolerance

        stable = stable_left & stable_right
        stable_streak = stable.groupby((stable != stable.shift()).cumsum()).transform('size')

        mask = (stable) & (stable_streak >= max_static_steps)
        df.loc[mask, 'is_valid'] = False
        filtered_data.set_data(df)

    @staticmethod
    def filter_async_pupil_size(filtered_data: FilteredData, tolerance: float = None):
        """
        Помечает строки, где зрачки изменяются в разные стороны с превышением допустимого отклонения.
        """
        df = filtered_data.get_data()

        if tolerance is None:
            tolerance = RawDataFilter.DEFAULT_TOLERANCE

        left_diff = df['eyeleft.pupildiameter'].diff()
        right_diff = df['eyeright.pupildiameter'].diff()

        mask = (
            (left_diff * right_diff < 0) &
            (left_diff.abs() > tolerance) &
            (right_diff.abs() > tolerance)
        )

        df.loc[mask, 'is_valid'] = False
        filtered_data.set_data(df)
