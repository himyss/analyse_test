import json

class CalibrationData:
    def __init__(self, user_id: int,
                 baseline_mean: float,
                 baseline_std: float,
                 positive_rise_speed: float,
                 negative_rise_speed: float,
                 positive_fall_speed: float,
                 negative_fall_speed: float,
                 neutral2_recovery_speed: float,
                 positive_mean_ratio: float,
                 negative_mean_ratio: float):
        """
        Контейнер для хранения результатов калибровки одного пользователя.

        Сохраняет:
        - baseline_mean и baseline_std: характеристики нормального спокойного состояния
        - positive_rise_speed и negative_rise_speed: скорость роста pupil size при стимуле
        - positive_fall_speed и negative_fall_speed: скорость восстановления pupil size после стимулов
        - neutral2_recovery_speed: скорость восстановления pupil size на нейтральной фазе
        - positive_mean_ratio: отношение среднего pupil size в Positive состоянии к baseline (>= 1.0)
        - negative_mean_ratio: отношение среднего pupil size в Negative состоянии к baseline (>= 1.0)
        """

        # Индивидуальные параметры пользователя
        self.user_id = user_id                      # Идентификатор пользователя
        self.baseline_mean = baseline_mean          # Средний размер зрачка в состоянии покоя
        self.baseline_std = baseline_std            # Стандартное отклонение pupil size вокруг baseline

        self.positive_rise_speed = positive_rise_speed  # Скорость роста pupil size на позитивном стимуле
        self.negative_rise_speed = negative_rise_speed  # Скорость роста pupil size на негативном стимуле

        self.positive_fall_speed = positive_fall_speed  # Скорость падения pupil size после позитивного стимулятора
        self.negative_fall_speed = negative_fall_speed  # Скорость падения pupil size после негативного стимулятора
        self.neutral2_recovery_speed = neutral2_recovery_speed  # Скорость восстановления pupil size на Neutral 2 фазе

        self.positive_mean_ratio = positive_mean_ratio  # Отношение среднего pupil size Positive к baseline
        self.negative_mean_ratio = negative_mean_ratio  # Отношение среднего pupil size Negative к baseline

    def to_dict(self) -> dict:
        """
        Преобразовать объект в словарь для сохранения или передачи.
        """
        return {
            'user_id': self.user_id,
            'baseline_mean': self.baseline_mean,
            'baseline_std': self.baseline_std,
            'positive_rise_speed': self.positive_rise_speed,
            'negative_rise_speed': self.negative_rise_speed,
            'positive_fall_speed': self.positive_fall_speed,
            'negative_fall_speed': self.negative_fall_speed,
            'neutral2_recovery_speed': self.neutral2_recovery_speed,
            'positive_mean_ratio': self.positive_mean_ratio,
            'negative_mean_ratio': self.negative_mean_ratio
        }

    def save(self, path: str):
        """
        Сохранить CalibrationData в JSON-файл.
        """
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f, indent=4)

    @staticmethod
    def load(path: str):
        """
        Загрузить CalibrationData из JSON-файла.
        """
        with open(path, 'r') as f:
            data = json.load(f)
        return CalibrationData(
            user_id=data['user_id'],
            baseline_mean=data['baseline_mean'],
            baseline_std=data['baseline_std'],
            positive_rise_speed=data['positive_rise_speed'],
            negative_rise_speed=data['negative_rise_speed'],
            positive_fall_speed=data['positive_fall_speed'],
            negative_fall_speed=data['negative_fall_speed'],
            neutral2_recovery_speed=data['neutral2_recovery_speed'],
            positive_mean_ratio=data['positive_mean_ratio'],
            negative_mean_ratio=data['negative_mean_ratio']
        )
