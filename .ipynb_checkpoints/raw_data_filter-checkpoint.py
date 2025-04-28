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
import pandas as pd
import numpy as np

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
        """
        Фильтрует события, между которыми слишком маленький интервал времени (< min_interval).
        Помечает обе строки: предыдущую и текущую.
        """
        df = filtered_data.get_data()
    
        delta_time = df['timestamp'].diff()
        outlier_mask = delta_time < min_interval
    
        bad_indexes = df.index[outlier_mask]
    
        df.loc[bad_indexes, 'is_valid'] = False
        df.loc[bad_indexes - 1, 'is_valid'] = False  # Помечаем и предыдущую строку
    
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

    @staticmethod
    def filter_pupil_speed(filtered_data, max_speed_mm_per_sec=5.0):
        """
        Фильтрация по скорости изменения pupil size только на валидных данных.
        """
        df = filtered_data.get_data()
    
        if 'avg_pupildiameter' not in df.columns or 'relative_timestamp' not in df.columns:
            raise ValueError("Нет необходимых колонок")
    
        # Работать только по валидным данным
        valid_data = df[df['is_valid']].copy()
    
        for i in range(1, len(valid_data)):
            delta_pupil = abs(valid_data.iloc[i]['avg_pupildiameter'] - valid_data.iloc[i-1]['avg_pupildiameter'])
            delta_time = valid_data.iloc[i]['relative_timestamp'] - valid_data.iloc[i-1]['relative_timestamp']
    
            if delta_time > 0:
                speed = delta_pupil / delta_time
                if speed > max_speed_mm_per_sec:
                    # Помечаем строки в оригинальном df через их индексы!
                    df.at[valid_data.index[i], 'is_valid'] = False
                    df.at[valid_data.index[i-1], 'is_valid'] = False
    
        filtered_data.set_data(df)

    @staticmethod
    def filter_by_std_outliers(fFilterData, time_window=1.0, sigma_threshold=2.0):
        """
        Фильтр: Отсев событий по выбросам std_pupildiameter во временном окне.

        :param fFilterData: объект FilteredData
        :param time_window: размер окна в секундах (±time_window вокруг каждой точки)
        :param sigma_threshold: порог в сигмах для отсева
        """
        df = fFilterData.get_data()

        if 'std_pupildiameter' not in df.columns:
            raise ValueError("std_pupildiameter не рассчитан. Сначала рассчитайте стандартное отклонение.")

        valid_data = df[df['is_valid']].copy()

        timestamps = valid_data['relative_timestamp'].values
        stds = valid_data['std_pupildiameter'].values

        outlier_mask = np.zeros(len(valid_data), dtype=bool)

        for i in range(len(valid_data)):
            current_time = timestamps[i]

            # Выбираем все точки в окне ±time_window
            mask = (timestamps >= current_time - time_window) & (timestamps <= current_time + time_window)
            local_stds = stds[mask]

            if len(local_stds) >= 2:
                mean_std = np.nanmean(local_stds)
                std_std = np.nanstd(local_stds)

                lower_bound = mean_std - sigma_threshold * std_std
                upper_bound = mean_std + sigma_threshold * std_std

                # Проверяем текущее значение
                if not (lower_bound <= stds[i] <= upper_bound):
                    outlier_mask[i] = True

        # Обновляем флаг is_valid для исходных данных
        valid_indices = valid_data.index
        df.loc[valid_indices[outlier_mask], 'is_valid'] = False

        # Обновляем данные в объекте
        fFilterData.set_data(df)




