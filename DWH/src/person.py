from datetime import datetime, date
import numpy as np
from random import randint

def import_file(full_name, path):
    """
    Импорт модулей Python из указанной директории
    """
    from importlib import util

    spec = util.spec_from_file_location(full_name, path)
    mod = util.module_from_spec(spec)

    spec.loader.exec_module(mod)
    return mod

STORE_START_DATE = datetime.combine(date(2023,1,1), datetime.min.time()) # формат даты datetime
END_DATE = datetime.now().date()
MAX_PERSON_AGE = 87


class Person:
    def __init__(self,
                 units,
                 registration_date_from=STORE_START_DATE,
                 registration_date_to=END_DATE,
                 **kwargs):

        self.units = units
        self.registration_date_from = registration_date_from
        self.registration_date_to = registration_date_to

        self.reset()

        if kwargs:  # если подаются данные - пробуем использовать их
            for k, v in kwargs.items():
                if k in self.fields:
                    self.__dict__[k] = v

        self.generate()

    def generate(self, reset=False):  # генерируем данные
        if reset:
            self.reset()

        # ФИО
        if randint(0, 1):  # мужской
            if isinstance(self.first_name, float):
                if np.isnan(self.first_name):
                    self.first_name = self.units.m_first_names['Имя'].values[randint(0, len(self.units.m_first_names) - 1)]

            if isinstance(self.second_name, float):
                if np.isnan(self.second_name):
                    self.second_name = self.units.m_second_names['Фамилия'].values[randint(0, len(self.units.m_second_names) - 1)]

            if isinstance(self.surname, float):
                if np.isnan(self.surname):
                    self.surname = self.units.m_surnames['Отчество'].values[randint(0, len(self.units.m_surnames) - 1)]

        else:  # женский
            if isinstance(self.first_name, float):
                if np.isnan(self.first_name):
                    self.first_name = self.units.f_first_names['Имя'].values[randint(0, len(self.units.f_first_names) - 1)]

            if isinstance(self.second_name, float):
                if np.isnan(self.second_name):
                    self.second_name = self.units.f_second_names['Фамилия'].values[randint(0, len(self.units.f_second_names) - 1)]

            if isinstance(self.surname, float):
                if np.isnan(self.surname):
                    self.surname = self.units.f_surnames['Отчество'].values[randint(0, len(self.units.f_surnames) - 1)]

        # локация
        ind = randint(0, self.units.cities.shape[0] - 1)

        if isinstance(self.region, float):
            if np.isnan(self.region):
                self.region = self.units.cities['Регион'].values[ind]

        if isinstance(self.city, float):
            if np.isnan(self.city):
                self.city = self.units.cities['Город'].values[ind]

        if isinstance(self.address, float):
            if np.isnan(self.address):
                sub_address = self.units.places[
                    (self.units.places['Регион'] == self.region) & (self.units.places['Город'] == self.city)]
                ind = randint(0, sub_address.shape[0] - 1)

                self.address = sub_address['Адрес'].values[ind]

                # даты регистрации и рождения
        if isinstance(self.registered_at, float):
            if np.isnan(self.registered_at):
                self.registered_at = self.units.random_time(self.registration_date_from,
                                                       self.registration_date_to)  # поправить на текущую неделю

        if isinstance(self.birthdate, float):
            if np.isnan(self.birthdate):
                birthdate_from_date = date(self.registration_date_to.year - MAX_PERSON_AGE,
                                           self.registration_date_to.month, self.registration_date_to.day)
                birthdate_from = datetime.combine(birthdate_from_date, datetime.min.time())  # формат даты datetime

                while True:  # на момент регистрации исполнилось 18 лет
                    self.birthdate = self.units.random_time(birthdate_from, self.registered_at).date()

                    if self.registered_at.year - self.birthdate.year > 18:
                        break
                    elif self.registered_at.year - self.birthdate.year == 18:
                        if self.registered_at.month >= self.birthdate.month:
                            break
                        elif self.registered_at.month == self.birthdate.month:
                            if self.registered_at.day > self.birthdate.day:
                                break

        # категория пользователей
        i = randint(0, 100)

        if i <= 10:
            self.category = 0
        elif i <= 30:
            self.category = 1
        elif i <= 80:
            self.category = 2
        elif i <= 97:
            self.category = 3
        elif i > 97:
            self.category = 4

    def reset(self):  # сбрасываем на нулевые значения
        self.first_name = np.nan
        self.second_name = np.nan
        self.surname = np.nan

        self.region = np.nan
        self.city = np.nan
        self.address = np.nan

        self.birthdate = np.nan
        self.registered_at = np.nan

        self.category = np.nan

    @property
    def fields(self):  # список допустимых параметров
        return [
            'first_name',
            'second_name',
            'surname',
            'region',
            'city',
            'address',
            'birthdate',
            'registered_at',
            'category'
        ]