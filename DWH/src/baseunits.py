from datetime import datetime, date, timedelta
import platform
import os
from random import randrange


CRC_FOLDER = f'{os.path.expanduser("~")}\\de_course__task10\\DWH\\src\\' if platform.system() == 'Windows' else '/opt/src/'
DATA_FOLDER = f'{os.path.expanduser("~")}\\de_course__task10\\DWH\\src\\data\\' if platform.system() == 'Windows' else r'/opt/src/data/'
CREDENTIALS_FILE = f'{CRC_FOLDER}de-course-etl-e7c2444a44f6.json'  # Имя файла с закрытым ключом, вы должны подставить свое
SPREADSHEET_ID = '1DMKQwUrU2OJNpsK0GJhK7pB7OJ-tRwUdVWmAyjAFDYM'  #  id гугл-таблицы с элементами для генерации синтетических данных


def import_file(full_name, path):
    """
    Импорт модулей Python из указанной директории
    """
    from importlib import util

    spec = util.spec_from_file_location(full_name, path)
    mod = util.module_from_spec(spec)

    spec.loader.exec_module(mod)
    return mod


class BaseUnits:
    def __init__(self):
        self.g_sheets = import_file('GSheet', f'{CRC_FOLDER}gsheet.py').GSheet(CREDENTIALS_FILE, SPREADSHEET_ID)

        self._m_first_names = False
        self._f_first_names = False

        self._m_second_names = False
        self._f_second_names = False

        self._m_surnames = False
        self._f_surnames = False

        self._cities = False
        self._places = False
        self._goods = False

    def random_time(self, start, end):
        if isinstance(start, date):
            start = datetime.combine(start, datetime.min.time())
        if isinstance(end, date):
            end = datetime.combine(end, datetime.min.time())

        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return start + timedelta(seconds=random_second)

    @property
    def m_first_names(self):
        if isinstance(self._m_first_names, bool):
            first_names = self.g_sheets.page('Имена')
            self._m_first_names = first_names[first_names['Пол'] == 'м']
            self._f_first_names = first_names[first_names['Пол'] == 'ж']
        return self._m_first_names

    @property
    def f_first_names(self):
        if isinstance(self._f_first_names, bool):
            first_names = self.g_sheets.page('Имена')
            self._m_first_names = first_names[first_names['Пол'] == 'м']
            self._f_first_names = first_names[first_names['Пол'] == 'ж']
        return self._f_first_names

    @property
    def m_second_names(self):
        if isinstance(self._m_second_names, bool):
            second_names = self.g_sheets.page('Фамилии')
            self._m_second_names = second_names[second_names['Пол'] == 'м']
            self._f_second_names = second_names[second_names['Пол'] == 'ж']
        return self._m_second_names

    @property
    def f_second_names(self):
        if isinstance(self._f_second_names, bool):
            second_names = self.g_sheets.page('Фамилии')
            self._m_second_names = second_names[second_names['Пол'] == 'м']
            self._f_second_names = second_names[second_names['Пол'] == 'ж']
        return self._f_second_names

    @property
    def m_surnames(self):
        if isinstance(self._m_surnames, bool):
            surnames = self.g_sheets.page('Отчества')
            self._m_surnames = surnames[surnames['Пол'] == 'м']
            self._f_surnames = surnames[surnames['Пол'] == 'ж']
        return self._m_surnames

    @property
    def f_surnames(self):
        if isinstance(self._f_surnames, bool):
            surnames = self.g_sheets.page('Отчества')
            self._m_surnames = surnames[surnames['Пол'] == 'м']
            self._f_surnames = surnames[surnames['Пол'] == 'ж']
        return self._f_surnames

    @property
    def cities(self):
        if isinstance(self._cities, bool):
            self._cities = self.g_sheets.page('Города')
        return self._cities

    @property
    def places(self):
        if isinstance(self._places, bool):
            self._places = self.g_sheets.page('Адреса')
        return self._places

    @property
    def goods(self):
        if isinstance(self._goods, bool):
            self._goods = self.g_sheets.page('Товары')
        return self._goods