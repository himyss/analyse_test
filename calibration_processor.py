import json
from calibration_data import CalibrationData

class CalibrationProcessor:
    @staticmethod
    def load_time_ranges(json_path: str, user_id: int) -> dict:
        """
        Загрузить тайминги фаз из JSON в формате:
        {
          "704": [
            { "shelf": "Baseline",  "time_range": [0.0, 30.0] },
            { "shelf": "Positive",  "time_range": [30.0, 60.0] },
            { "shelf": "Neutral_2",   "time_range": [60.0, 90.0] },
            { "shelf": "Negative",  "time_range": [90.0, 120.0] }
          ],
          ...
        }
        Возвращает dict вида {"Baseline": [...], "Positive": [...], ...}
        """
        with open(json_path, 'r', encoding='utf-8') as f:
            all_timings = json.load(f)

        if str(user_id) not in all_timings:
            raise ValueError(f"No timings for user {user_id}")
        return {
            item['shelf']: item['time_range']
            for item in all_timings[str(user_id)]
        }

    @staticmethod
    def compute_baseline(filtered_data, time_ranges: dict) -> tuple[float, float]:
        df = filtered_data.get_data()
        t0, t1 = time_ranges['Baseline']
        mask = (
            (df['is_valid']) &
            (df['relative_timestamp'] >= t0) &
            (df['relative_timestamp'] <= t1)
        )
        segment = df.loc[mask, 'avg_pupildiameter_smooth']
        if segment.empty:
            raise ValueError(f"No valid data in baseline [{t0}, {t1}]")
        return segment.mean(), segment.std(ddof=1)

    @staticmethod
    def compute_positive_rise_speed(filtered_data, time_ranges: dict, baseline_mean: float) -> float:
        df = filtered_data.get_data()
        t0, t1 = time_ranges['Positive']
        seg = df.loc[
            (df['is_valid']) &
            (df['relative_timestamp'] >= t0) &
            (df['relative_timestamp'] <= t1),
            ['relative_timestamp', 'avg_pupildiameter_smooth']
        ]
        if seg.empty:
            raise ValueError(f"No valid data in positive [{t0}, {t1}]")
        # момент пика
        idx_peak = seg['avg_pupildiameter_smooth'].idxmax()
        t_peak   = seg.loc[idx_peak, 'relative_timestamp']
        val_peak = seg.loc[idx_peak, 'avg_pupildiameter_smooth']
        return (val_peak - baseline_mean) / (t_peak - t0)

    @staticmethod
    def compute_positive_fall_speed(filtered_data, time_ranges: dict, baseline_mean: float) -> float:
        df = filtered_data.get_data()
        t0, t1 = time_ranges['Positive']
        seg = df.loc[
            (df['is_valid']) &
            (df['relative_timestamp'] >= t0) &
            (df['relative_timestamp'] <= t1),
            ['relative_timestamp', 'avg_pupildiameter_smooth']
        ]
        if seg.empty:
            raise ValueError(f"No valid data in positive [{t0}, {t1}]")
        idx_peak = seg['avg_pupildiameter_smooth'].idxmax()
        t_peak   = seg.loc[idx_peak, 'relative_timestamp']
        val_peak = seg.loc[idx_peak, 'avg_pupildiameter_smooth']
        # значение в конце фазы (берём последнее по времени)
        idx_end  = seg['relative_timestamp'].idxmax()
        t_end    = t1
        val_end  = seg.loc[idx_end, 'avg_pupildiameter_smooth']
        return (val_peak - val_end) / (t_end - t_peak)

    @staticmethod
    def compute_negative_rise_speed(filtered_data, time_ranges: dict, baseline_mean: float) -> float:
        df = filtered_data.get_data()
        t0, t1 = time_ranges['negative']
        seg = df.loc[
            (df['is_valid']) &
            (df['relative_timestamp'] >= t0) &
            (df['relative_timestamp'] <= t1),
            ['relative_timestamp', 'avg_pupildiameter_smooth']
        ]
        if seg.empty:
            raise ValueError(f"No valid data in negative [{t0}, {t1}]")
        idx_peak = seg['avg_pupildiameter_smooth'].idxmax()
        t_peak   = seg.loc[idx_peak, 'relative_timestamp']
        val_peak = seg.loc[idx_peak, 'avg_pupildiameter_smooth']
        return (val_peak - baseline_mean) / (t_peak - t0)

    @staticmethod
    def compute_negative_fall_speed(filtered_data, time_ranges: dict, baseline_mean: float) -> float:
        df = filtered_data.get_data()
        t0, t1 = time_ranges['Negative']
        seg = df.loc[
            (df['is_valid']) &
            (df['relative_timestamp'] >= t0) &
            (df['relative_timestamp'] <= t1),
            ['relative_timestamp', 'avg_pupildiameter_smooth']
        ]
        if seg.empty:
            raise ValueError(f"No valid data in negative [{t0}, {t1}]")
        idx_peak = seg['avg_pupildiameter_smooth'].idxmax()
        t_peak   = seg.loc[idx_peak, 'relative_timestamp']
        val_peak = seg.loc[idx_peak, 'avg_pupildiameter_smooth']
        idx_end  = seg['relative_timestamp'].idxmax()
        t_end    = t1
        val_end  = seg.loc[idx_end, 'avg_pupildiameter_smooth']
        return (val_peak - val_end) / (t_end - t_peak)

    @staticmethod
    def compute_neutral2_recovery_speed(filtered_data, time_ranges: dict, baseline_mean: float) -> float:
        df = filtered_data.get_data()
        t0, t1 = time_ranges['Neutral_2']
        seg = df.loc[
            (df['is_valid']) &
            (df['relative_timestamp'] >= t0) &
            (df['relative_timestamp'] <= t1),
            ['relative_timestamp', 'avg_pupildiameter_smooth']
        ]
        if seg.empty:
            raise ValueError(f"No valid data in neutral2 [{t0}, {t1}]")
        idx_start = seg['relative_timestamp'].idxmin()
        idx_end   = seg['relative_timestamp'].idxmax()
        val_start = seg.loc[idx_start, 'avg_pupildiameter_smooth']
        val_end   = seg.loc[idx_end,   'avg_pupildiameter_smooth']
        return (val_start - val_end) / (t1 - t0)

    @staticmethod
    def compute_positive_mean_ratio(filtered_data, time_ranges: dict, baseline_mean: float) -> float:
        df = filtered_data.get_data()
        t0, t1 = time_ranges['Positive']
        seg = df.loc[
            (df['is_valid']) &
            (df['relative_timestamp'] >= t0) &
            (df['relative_timestamp'] <= t1),
            'avg_pupildiameter_smooth'
        ]
        if seg.empty:
            raise ValueError(f"No valid data in positive [{t0}, {t1}]")
        return seg.mean() / baseline_mean

    @staticmethod
    def compute_negative_mean_ratio(filtered_data, time_ranges: dict, baseline_mean: float) -> float:
        df = filtered_data.get_data()
        t0, t1 = time_ranges['Positive']
        seg = df.loc[
            (df['is_valid']) &
            (df['relative_timestamp'] >= t0) &
            (df['relative_timestamp'] <= t1),
            'avg_pupildiameter_smooth'
        ]
        if seg.empty:
            raise ValueError(f"No valid data in negative [{t0}, {t1}]")
        return seg.mean() / baseline_mean

    @staticmethod
    def process(filtered_data, user_id: int, json_path: str) -> CalibrationData:
        time_ranges = CalibrationProcessor.load_time_ranges(json_path, user_id)
        baseline_mean, baseline_std = CalibrationProcessor.compute_baseline(filtered_data, time_ranges)
        pos_rise = CalibrationProcessor.compute_positive_rise_speed(filtered_data, time_ranges, baseline_mean)
        pos_fall = CalibrationProcessor.compute_positive_fall_speed(filtered_data, time_ranges, baseline_mean)
        neg_rise = CalibrationProcessor.compute_negative_rise_speed(filtered_data, time_ranges, baseline_mean)
        neg_fall = CalibrationProcessor.compute_negative_fall_speed(filtered_data, time_ranges, baseline_mean)
        neutral2_r = CalibrationProcessor.compute_neutral2_recovery_speed(filtered_data, time_ranges, baseline_mean)
        pos_ratio = CalibrationProcessor.compute_positive_mean_ratio(filtered_data, time_ranges, baseline_mean)
        neg_ratio = CalibrationProcessor.compute_negative_mean_ratio(filtered_data, time_ranges, baseline_mean)

        return CalibrationData(
            user_id=user_id,
            baseline_mean=baseline_mean,
            baseline_std=baseline_std,
            positive_rise_speed=pos_rise,
            negative_rise_speed=neg_rise,
            positive_fall_speed=pos_fall,
            negative_fall_speed=neg_fall,
            neutral2_recovery_speed=neutral2_r,
            positive_mean_ratio=pos_ratio,
            negative_mean_ratio=neg_ratio
        )
