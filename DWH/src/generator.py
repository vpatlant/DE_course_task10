import os
import platform
from datetime import datetime, date, timedelta
import pandas as pd
from random import randint
import psycopg2
from sqlalchemy import create_engine


STORE_START_DATE = datetime.combine(date(2023,1,1), datetime.min.time()) # формат даты datetime
END_DATE = datetime.now().date()

ANALYTICS_START_DATE = date(2023,9,1)
ANALYTICS_END_DATE = date(2024,9,1)

START_PERSON_COUNT = 72700
FINAL_PERSON_COUNT = 142900

MAX_PERSON_AGE = 87
PERSON_CATEGORIES = {
    '0': ('0 покупок', (0, 0), '10%'),
    '1': ('<=5 покупок', (1, 5), '20%'),
    '2': ('<=10 покупок', (6, 10), '50%'),
    '3': ('<=15 покупок', (11, 15), '17%'),
    '4': ('<=20 покупок', (15, 20), '3%'),
}

CRC_FOLDER = f'{os.path.expanduser("~")}\\de_course__task10\\DWH\\src\\' if platform.system() == 'Windows' else '/opt/src/'
DATA_FOLDER = f'{os.path.expanduser("~")}\\de_course__task10\\DWH\\src\\data\\' if platform.system() == 'Windows' else r'/opt/src/data/'

POSTGRES_HOST = 'localhost' if platform.system() == 'Windows' else 'postgres_dev'
POSTGRES_PORT = '7432' if platform.system() == 'Windows' else '5432'

def import_file(full_name, path):
    """
    Импорт модулей Python из указанной директории
    """
    from importlib import util

    spec = util.spec_from_file_location(full_name, path)
    mod = util.module_from_spec(spec)

    spec.loader.exec_module(mod)
    return mod

#units = import_file('BaseUnits', '/opt/src/baseunits.py').BaseUnits()

class Generator:
    def __init__(self, units, ti_date):

        self.units = units
        tuesday_of_current_week = ti_date - timedelta(days=(ti_date.weekday() - 1))  # вторник текущей недели
        first_date_of_week = tuesday_of_current_week - timedelta(days=8)  # понедельник предыдущей недели

        last_date_of_week = tuesday_of_current_week - timedelta(days=1)  # понедельник текущей недели

        if (first_date_of_week < ANALYTICS_START_DATE and last_date_of_week > ANALYTICS_START_DATE) \
                or (
                first_date_of_week >= ANALYTICS_START_DATE and last_date_of_week <= ANALYTICS_END_DATE) \
                or (first_date_of_week < ANALYTICS_END_DATE and last_date_of_week > ANALYTICS_END_DATE):

            print('генерируем данные')

            # инициализация таблиц
            self.create_customers_table()
            self.create_orders_table()

            if first_date_of_week <= ANALYTICS_END_DATE <= last_date_of_week:
                print('генерация пользователей с датой регистрации финальной недели исследования')

                current_customers_count = self.sql_request('select count(*) from customers;')[0][
                    0]  # текущее количество пользователей
                self.customers_count = FINAL_PERSON_COUNT - current_customers_count

                self.delete_from_customers_table(
                    f"registered_at between '{first_date_of_week}' and '{ANALYTICS_END_DATE}';")

                last_users = pd.DataFrame(
                    data=self.generate_customers_list(registration_start=first_date_of_week,
                                                      registration_end=ANALYTICS_END_DATE))

                print('генерация пользователей с датой регистрации финальной недели исследования', first_date_of_week,
                      ANALYTICS_END_DATE)
                self.sql_append('customers', last_users)
                print(f'added {len(last_users)} row(s)')

                print('генерация заказов с датой финальной недели исследования', first_date_of_week, ANALYTICS_END_DATE)
                current_orders_count = self.sql_request('select count(*) from orders;')[0][
                    0]  # текущее количество заказов
                self.orders_count = 1000000 - current_orders_count

                self.delete_from_orders_table(f"sale_time between '{first_date_of_week}' and '{ANALYTICS_END_DATE}';")

                last_orders = pd.DataFrame(
                    data=self.generate_orders_list(sale_date_from=first_date_of_week,
                                                   sale_date_to=ANALYTICS_END_DATE))

                for region in last_orders['region'].unique():
                    fname = f'{DATA_FOLDER}{region}_{ti_date.year}_{ti_date.isocalendar().week}.csv'
                    last_orders[last_orders['region'] == region].to_csv(fname, index=False)

            else:
                # пользователи "появившиеся" до начала исследования
                if first_date_of_week <= ANALYTICS_START_DATE <= last_date_of_week:
                    print('генерация пользователей с датой регистрации до даты исследования')

                    self.customers_count = START_PERSON_COUNT

                    self.delete_from_customers_table(
                        f"registered_at between '{STORE_START_DATE}' and '{ANALYTICS_START_DATE}';")

                    old_users = pd.DataFrame(
                        data=self.generate_customers_list(registration_start=STORE_START_DATE,
                                                          registration_end=ANALYTICS_START_DATE))

                    print('генерация пользователей с датой регистрации до даты исследования', STORE_START_DATE,
                          ANALYTICS_START_DATE)
                    self.sql_append('customers', old_users)
                    print(f'added {len(old_users)} row(s)')

                # сдвиг даты начала генерации данных для первой недели
                generate_from_date = ANALYTICS_START_DATE if first_date_of_week <= ANALYTICS_START_DATE else first_date_of_week

                print('генерация пользователей текущей недели', generate_from_date, last_date_of_week)

                customers_mean = (FINAL_PERSON_COUNT - START_PERSON_COUNT) // (365 / 7)
                print('customers_mean', customers_mean)

                self.customers_count = int(randint(int(customers_mean * 0.9), int(customers_mean * 1.1)))
                print('customers_count', self.customers_count)

                self.delete_from_customers_table(
                    f"registered_at between '{generate_from_date}' and '{last_date_of_week}';")
                print('cleared outdated customers')

                new_users = pd.DataFrame(
                    data=self.generate_customers_list(registration_start=generate_from_date,
                                                      registration_end=last_date_of_week))
                print('new_users', len(new_users))

                self.sql_append('customers', new_users)
                print(f'added {len(new_users)} row(s)')

                print('генерация заказов текущей недели')

                orders_count_mean = 1000000 // (365 / 7)

                self.orders_count = int(randint(int(orders_count_mean * 0.9), int(orders_count_mean * 1.1)))
                print('self.orders_count', self.orders_count)

                self.delete_from_orders_table(
                    f"sale_time between '{generate_from_date}' and '{last_date_of_week}';")

                new_orders = pd.DataFrame(
                    data=self.generate_orders_list(sale_date_from=generate_from_date,
                                                   sale_date_to=last_date_of_week))

                if ti_date.isocalendar().week == 20:
                    new_orders = new_orders[new_orders['region'] != 'North']
                if ti_date.isocalendar().week == 21:
                    new_orders = new_orders[new_orders['region'] != 'South']
                if ti_date.isocalendar().week == 22:
                    new_orders = new_orders[new_orders['region'] != 'East']
                if ti_date.isocalendar().week == 23:
                    new_orders = new_orders[new_orders['region'] != 'West']

                for region in new_orders['region'].unique():
                    fname = f'{DATA_FOLDER}{region}_{ti_date.year}_{ti_date.isocalendar().week}.csv'

                    new_orders[new_orders['region'] == region].to_csv(fname, index=False)

        else:
            print('не надо генерировать')

    def sql_command(self, sql):
        with psycopg2.connect(user="user",
                              password="password",
                              host=POSTGRES_HOST,
                              port=POSTGRES_PORT,
                              database="postgres_dev") as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    conn.commit()
            except Exception as e:
                print(str(e))

    def sql_request(self, sql):
        with psycopg2.connect(user="user",
                              password="password",
                              host=POSTGRES_HOST,
                              port=POSTGRES_PORT,
                              database="postgres_dev") as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    res = cursor.fetchall()
                return res
            except Exception as e:
                print(str(e))

    def sql_append(self, table, df):
        db = create_engine(f'postgresql+psycopg2://user:password@{POSTGRES_HOST}:{POSTGRES_PORT}/postgres_dev')
        conn = db.connect()
        try:
            df.to_sql(table, conn, if_exists='append', index=False)

        except Exception as e:
            print(str(e))
        conn.close()

    def create_customers_table(self):
        sql = """
            CREATE TABLE IF NOT EXISTS customers (
            id serial PRIMARY KEY,
            first_name varchar(50),
            second_name varchar(50),
            surname varchar(50),
            region varchar(50),
            city varchar(50),
            address varchar(200),
            birthdate date,
            registered_at timestamp,
            category int
            )
        """
        self.sql_command(sql)

    def create_orders_table(self):
        sql = """
            CREATE TABLE IF NOT EXISTS orders (
            id serial PRIMARY KEY,
            customer_id int,
            product_id varchar(20),
            quantity int,
            sale_amount float4,
            region varchar(50),
            sale_time timestamp,
            CONSTRAINT orders_customers_fk FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON
            DELETE CASCADE
            )
        """
        self.sql_command(sql)

    def delete_from_customers_table(self, limit):
        self.sql_command(f'DELETE FROM customers WHERE {limit}')

    def delete_from_orders_table(self, limit):
        self.sql_command(f'DELETE FROM orders WHERE {limit}')

    def generate_customers_list(self, registration_start=None, registration_end=None):
        # генерация пользователей пропорционально количеству жителей в городах
        customer_list = []

        cities_mod = self.units.cities.copy()
        cities_mod['Население'] = cities_mod['Население'].astype(int)
        cities_mod["cum_sum"] = cities_mod["Население"].cumsum()

        max_ = cities_mod["cum_sum"].max()
        print('max_', max_)
        for customer_ in range(0, self.customers_count):
            val_ = randint(1, max_)
            if val_ < cities_mod.iloc[0]['cum_sum']:
                city = cities_mod[:1][['Регион', 'Город']]
            elif val_ > cities_mod.iloc[-2]['cum_sum']:
                city = cities_mod[-1:][['Регион', 'Город']]
            else:
                city = cities_mod[cities_mod.index == max(cities_mod[cities_mod['cum_sum'] < val_].index)][
                    ['Регион', 'Город']]

            if registration_start:
                registered_at = self.units.random_time(registration_start, registration_end)

            person = import_file('Person', f'{CRC_FOLDER}person.py').Person(
                self.units,
                registration_start,
                registration_end,
                region=city['Регион'].values[0],
                city=city['Город'].values[0],
                registered_at=registered_at
            )
            person_data = person.__dict__
            del person_data['units']
            del person_data['registration_date_from']
            del person_data['registration_date_to']
            # print(person_data)

            customer_list.append(person_data)

        print('customer_list', len(customer_list))
        return customer_list

    def generate_order(self, active_users, sale_date_from, sale_date_to):
        # уникальный идентификатор продажи
        # sale_id - autoincrement

        # идентификатор клиента
        customer_id = randint(0, len(active_users) - 1)  # количество активных пользователей
        # print(customer_id)
        user_params = active_users.iloc[customer_id]
        # print(user_params)

        # идентификатор продукта
        product_ind = randint(0, self.units.goods.shape[0] - 1)
        product_id = self.units.goods[self.units.goods.index == product_ind]['sku'].values[0]

        # количество купленных товаров
        quantity = randint(1, 15)

        # дата продажи
        interval_start = datetime.combine(sale_date_from, datetime.min.time())  # полночь
        interval_end = datetime.combine(sale_date_to, datetime.min.time())  # полночь отчетной даты

        sale_time = self.units.random_time(interval_start, interval_end)

        # сумма продажи, рассчитывается как количество товаров * случайная цена товара (заменена на фиксированную из прайса)
        sale_amount = quantity * float(
            self.units.goods[self.units.goods.index == product_ind]['Цена'].values[0].replace(',', '.'))

        region = ''  # регион клиента, один из: North, South, East, West
        if user_params['region'] == 'Нижегородская обл':
            region = 'North'
        elif user_params['region'] == 'Рязанская обл':
            region = 'West'
        elif user_params['region'] == 'респ Татарстан':
            region = 'South'
        else:
            region = 'East'

        return {
            'customer_id': user_params['customer_id'],
            'product_id': product_id,
            'quantity': quantity,
            'sale_amount': sale_amount,
            'region': region,
            'sale_time': sale_time
        }

    def generate_orders_list(self, sale_date_from, sale_date_to):
        orders_list = []

        # список активных пользователей и параметры для заказов
        sql = """
            SELECT
            	c.id AS customer_id,
            	c.region AS region,
            	c.category AS category,
            	count(o.id) AS orders_count
            FROM
            	customers c
            LEFT JOIN orders o ON
            	c.id = o.customer_id
            GROUP BY
            	c.id
            HAVING
            	count(o.id) < c.category * 5
        """
        active_users = pd.DataFrame(data=self.sql_request(sql),
                                    columns=['customer_id', 'region', 'category', 'orders_count'])

        for i in range(self.orders_count):
            while True:
                order = self.generate_order(active_users, sale_date_from, sale_date_to)
                if active_users[active_users['customer_id'] == order['customer_id']]['orders_count'].values[0] < \
                        active_users[active_users['customer_id'] == order['customer_id']]['category'].values[0] * 5:
                    break

            active_users.loc[active_users['customer_id'] == order['customer_id'], 'orders_count'] = \
            active_users[active_users['customer_id'] == order['customer_id']]['orders_count'].values[0] + 1
            orders_list.append(order)
            if i % 500 == 0:
                print(f'{i} / {self.orders_count}')

        return orders_list