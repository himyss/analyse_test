import pandas as pd
import gzip
import json
from pathlib import Path

class RawData:
    def __init__(self, gz_file_path, user_id=None):
        """
        Инициализация объекта для обработки данных.
        :param gz_file_path: путь к файлу .gz
        :param user_id: идентификатор пользователя (например, 704)
        """
        self._user_id = user_id
        self._raw_data = None # types annotation
        
        self.read_gz(gz_file_path) 

    def read_gz(self, gz_file_path):
        """
        Чтение .gz файла, содержащего построчные JSON объекты.
        Преобразование в таблицу pandas.DataFrame.
        """
        with gzip.open(gz_file_path, 'rt', encoding='utf-8') as f:
            content = f.read()
            json_data = [json.loads(line) for line in content.strip().split('\n') if line]
            self._raw_data = pd.json_normalize(json_data)

    def get_raw_data(self):
        """
        Возвращает сырые данные в формате pandas.DataFrame.
        """
        return self._raw_data

    def get_user_id(self):
        """
        Возвращает идентификатор пользователя.
        """
        return self._user_id
