import pandas as pd
import numpy as np

class FilteredData:
    _calculated_fields = ['relative_timestamp', 'avg_pupildiameter', 'std_pupildiameter', 'avg_pupildiameter_smooth']

    def __init__(self, raw_df: pd.DataFrame = None, user_id: int = None):
        """
        Контейнер для хранения отфильтрованных данных и метаданных эксперимента.
        """
        if raw_df is not None:
            self._data = raw_df.copy()
            self._prepare_data()
        else:
            self._data = None
            self._start_timestamp = None

        self._user_id = user_id

    def _prepare_data(self, time_window_smooth=0.3):
        """
        Подготовка новой таблицы данных:
        - Переименование колонок
        - Добавление флага 'is_valid' (если отсутствует)
        - Обнуление вычисляемых полей
        - Вычисление start_timestamp и относительного времени
        - Добавление человекочитаемого времени 'timestamp_readable'
        - Вычисление среднего размера зрачка (avg_pupildiameter)
        - Вычисление стандартного отклонения размера зрачка (std_pupildiameter)
        - Сглаживание среднего размера зрачка (avg_pupildiameter_smooth)
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

        # Вычисление среднего размера зрачка (если доступны оба глаза)
        if 'eyeleft.pupildiameter' in self._data.columns and 'eyeright.pupildiameter' in self._data.columns:
            self._data['avg_pupildiameter'] = (
                self._data['eyeleft.pupildiameter'] + self._data['eyeright.pupildiameter']
            ) / 2
        else:
            self._data['avg_pupildiameter'] = np.nan  # Если нет обоих глаз — заполняем NaN

        # Вычисление стандартного отклонения размера зрачка (по временному окну)
        
        if not valid_data.empty:
            time_window = 0.5  # Временное окно в секундах (±0.5 сек)
            timestamps = valid_data['relative_timestamp'].values
            pupils = valid_data['avg_pupildiameter'].values
        
            std_pupil_list = []
        
            for i in range(len(valid_data)):
                current_time = timestamps[i]
                mask = (timestamps >= current_time - time_window) & (timestamps <= current_time + time_window)
                window_pupils = pupils[mask]
        
                if len(window_pupils) >= 2:
                    std = np.std(window_pupils, ddof=1)
                else:
                    std = np.nan
        
                std_pupil_list.append(std)
        
            # Теперь вставляем рассчитанный столбец обратно
            self._data.loc[self._data['is_valid'] == True, 'std_pupildiameter'] = std_pupil_list
        else:
            self._data['std_pupildiameter'] = np.nan

        # Сглаживание среднего диаметра зрачка
        self._smooth_avg_pupildiameter(time_window_smooth)

    def _reset_calculated_fields(self):
        """
        Обнуление всех вычисляемых полей.
        Если поле отсутствует — создаётся с NaN.
        """
        for field in self._calculated_fields:
            if field not in self._data.columns:
                self._data[field] = np.nan

    def _smooth_avg_pupildiameter(self, time_window_smooth=0.3):
        """
        Сглаживание avg_pupildiameter по времени с использованием скользящего окна.
        """
        if 'timestamp' not in self._data.columns or 'avg_pupildiameter' not in self._data.columns:
            raise ValueError("В данных должны быть колонки 'timestamp' и 'avg_pupildiameter'.")
    
        # Сортировка по времени (на всякий случай)
        self._data = self._data.sort_values('timestamp').reset_index(drop=True)
    
        # Создаём маску для валидных строк с правильной длиной
        valid_mask = self._data['is_valid'] == True
    
        # Создаём пустую колонку
        self._data['avg_pupildiameter_smooth'] = np.nan
    
        # Получаем значения только для валидных данных
        timestamps = self._data.loc[valid_mask, 'timestamp'].values
        values = self._data.loc[valid_mask, 'avg_pupildiameter'].values
    
        smoothed_values = []
    
        for i, t in enumerate(timestamps):
            # Окно времени
            mask = (timestamps >= t - time_window_smooth) & (timestamps <= t + time_window_smooth)
            if np.any(mask):
                smoothed_values.append(np.mean(values[mask]))
            else:
                smoothed_values.append(values[i])  # fallback на само значение
    
        # Записываем сглаженные значения только для valid строк
        self._data.loc[valid_mask, 'avg_pupildiameter_smooth'] = smoothed_values

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

    # Валидация. TODO создать нормальную валидацию по всем отборам
    def validate_filters(self):
            """
            Проверяет, что после применения всех фильтров:
             - колонка 'is_valid' существует и не содержит NaN,
             - есть хотя бы одна строка, помеченная как невалидная (фильтры что-то отработали),
             - и при этом остаётся хотя бы одна валидная строка.
            В случае успеха печатает сообщение, иначе бросает ошибку или предупреждает.
            """
            if 'is_valid' not in self._data.columns:
                raise ValueError("Фильтры не применялись: нет колонки 'is_valid'.")
    
            # Проверяем на пропуски
            if self._data['is_valid'].isnull().any():
                cnt = int(self._data['is_valid'].isnull().sum())
                raise ValueError(f"В колонке 'is_valid' обнаружено {cnt} пропущенных значений.")
    
            total = len(self._data)
            valid_count   = int(self._data['is_valid'].sum())
            invalid_count = total - valid_count
    
            if invalid_count == 0:
                raise RuntimeError("Ни одна строка не была отфильтрована: проверьте работу фильтров.")
            if valid_count == 0:
                raise RuntimeError("Все строки оказались невалидными: слишком жёсткие фильтры или ошибка.")
    
            print(f"Данные успешно отфильтрованы: {valid_count} валидных, {invalid_count} невалидных из {total} строк.")
    
