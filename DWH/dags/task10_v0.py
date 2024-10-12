from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import os
import platform
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import psycopg2
import clickhouse_connect


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

DATA_FOLDER = fr'/opt/src/'

CRC_FOLDER = f'{os.path.expanduser("~")}\\de_course__task10\\DWH\\src\\' if platform.system() == 'Windows' else '/opt/src/'
DATA_FOLDER = f'{os.path.expanduser("~")}\\de_course__task10\\DWH\\src\\data\\' if platform.system() == 'Windows' else r'/opt/src/data/'
CREDENTIALS_FILE = f'{CRC_FOLDER}de-course-etl-e7c2444a44f6.json'  # Имя файла с закрытым ключом, вы должны подставить свое
SPREADSHEET_ID = '1DMKQwUrU2OJNpsK0GJhK7pB7OJ-tRwUdVWmAyjAFDYM'  #  id гугл-таблицы с элементами для генерации синтетических данных
JARS_PATH = f'{CRC_FOLDER}postgresql-42.3.9.jar'

POSTGRES_HOST = 'localhost' if platform.system() == 'Windows' else 'postgres_dev'
POSTGRES_PORT = '7432' if platform.system() == 'Windows' else '5432'
CLICKHOUSE_HOST = 'localhost' if platform.system() == 'Windows' else 'clickhouse_dev'
CLICKHOUSE_PORT = '7433' if platform.system() == 'Windows' else '8123'

TG_BOT_TOKEN = 'tg_bot_token'
TG_CHANNEL = '@channel_for_error'


def postgres_command_py(sql):
    with psycopg2.connect(user="user",
                          password="password",
                          host=POSTGRES_HOST,
                          port=POSTGRES_PORT,
                          database="postgres_dev") as conn:

        print(sql)

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                conn.commit()
        except Exception as e:
            print(str(e))

def click_command_py(sql):
        try:
            with  clickhouse_connect.get_client(
                    username='user',
                    password='password',
                    host=CLICKHOUSE_HOST,
                    port=CLICKHOUSE_PORT) as client:

                print(sql)

                client.command(sql)

        except Exception as e:
            print(str(e))

def create_orders_agg_table_py():
    sql = """
    CREATE TABLE IF NOT EXISTS orders_agg (
    id serial PRIMARY KEY,
    region varchar(50),
    product_id varchar(20),
    quantity int,
    sale_amount float4,
    aov float4,
    "date" Date
    )
    """
    postgres_command_py(sql)

def create_click_orders_agg_table_py():
    sql = """
    CREATE TABLE IF NOT EXISTS clickhouse_dev.orders_agg (
    region String,
    product_id String,
    quantity Int16,
    sale_amount Float32,
    aov Float32,
    "date" Date,
	load_at DateTime
    )
    ENGINE = MergeTree
	PARTITION BY "date"
	ORDER BY ("date", region, product_id)
    """
    
    click_command_py(sql)

def clear_orders_agg_py(**kwargs):
    ti_date = datetime.strptime(kwargs["ds"], '%Y-%m-%d').date()

    sql = f''' delete from orders_agg where "date" = '{ti_date}'; '''

    postgres_command_py(sql)

def clear_click_orders_agg_py(**kwargs):
    ti_date = datetime.strptime(kwargs["ds"], '%Y-%m-%d').date()

    sql = f''' ALTER TABLE clickhouse_dev.orders_agg DROP PARTITION '{ti_date}'; '''

    click_command_py(sql)

def generator_py(**kwargs):
    ti_date = datetime.strptime(kwargs["ds"], '%Y-%m-%d').date()
    print('ti_date', ti_date)

    units = import_file('BaseUnits', f'{CRC_FOLDER}baseunits.py').BaseUnits()
    print('units.places', len(units.places))

    generator = import_file('Generator', f'{CRC_FOLDER}generator.py').Generator(units, ti_date) #ti_date

def load_py(region, **kwargs):
    ti_date = datetime.strptime(kwargs["ds"], '%Y-%m-%d').date()
    print('ti_date', ti_date)

    fname = f'{DATA_FOLDER}{region}_{ti_date.year}_{ti_date.isocalendar().week}.csv'
    print('check', fname)

    if os.path.isfile(fname):
        print('file exists')

        spark = (SparkSession.builder
            .appName(region)
            .config('spark.jars', JARS_PATH)
            .getOrCreate()
            )

        # читаем данные из csv
        df = spark.read.option("inferSchema", 'True').csv(
            fname,
            sep=',',
            header=True
        )

        df.printSchema()

        # проверяем отсутствие дубликатов и пропусков в данных
        before = df.count()
        print('before', before)

        df = df.na.drop()

        after = df.count()
        print('after', after)

        if after*100/before < 95:
            print('check source data!')

        else:
            # записываем данные в таблицу заказов
            (
            df
            .write
                .format('jdbc')
                .option('driver', 'org.postgresql.Driver')
                .option('url', f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/postgres_dev')
                .option('dbtable', 'orders')
                .option('user', 'user')
                .option('password', 'password')
                .mode('append')
            .  save()
            )


            aggregated = (df.groupBy("region","product_id")
                .agg(f.sum("quantity").alias("quantity"), 
                    f.sum("sale_amount").alias("sale_amount"),
                    f.avg("sale_amount").alias("aov"))
                .withColumn("date", f.to_date(f.lit(kwargs["ds"])))
            )

            aggregated.printSchema()

            aggregated.show(10)
            
            (aggregated
                .write
                    .format('jdbc')
                    .option('driver', 'org.postgresql.Driver')
                    .option('url', f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/postgres_dev')
                    .option('dbtable', 'orders_agg')
                    .option('user', 'user')
                    .option('password', 'password')
                    .mode('append')
                .save()
             )            


        spark.stop()
    else:
        print('there is no data file')

def copy_aggregates_py(**kwargs):
    ti_date = datetime.strptime(kwargs["ds"], '%Y-%m-%d').date()

    sql = f"""
    INSERT
    INTO
    clickhouse_dev.orders_agg
    SELECT
        region,
        product_id,
        quantity,
        sale_amount,
        aov,
        "date",
        Now() as load_at 
    FROM
    postgresql('{POSTGRES_HOST}:{POSTGRES_PORT}', 'postgres_dev', 'orders_agg', 'user', 'password')
    WHERE
    "date" = '{ti_date}'
    """

    click_command_py(sql)

def notify_on_failure(context):
    # Function to send a notification to Telegram when a task fails
    print('notify_on_failure function')
    task_execution_date = context.get('execution_date')
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id

    message = f"⚠️ Task failed in DAG: {dag_id}\n\nTask ID: {task_id}\nExecution Date:{task_execution_date}"
    print(message)

    tg = import_file('Tg', f'{CRC_FOLDER}tg.py').Tg(TG_BOT_TOKEN, TG_CHANNEL)
    tg.send_text(message)


dag = DAG(
    dag_id="de_course__task10",
    tags=[
        "de_course",
        "task10"
    ],
    start_date=datetime(2023,8,15),
    end_date=datetime(2024,9,20),
    schedule='45 12 * * 2',
    catchup=True,
    max_active_runs=1,
    on_failure_callback=notify_on_failure
)

start = EmptyOperator(task_id="start", dag=dag)

finish = EmptyOperator(task_id="finish", dag=dag)

generator = PythonOperator(
    task_id='generator',
    python_callable=generator_py,
    dag=dag
)

create_agg = PythonOperator(
    task_id='create_orders_agg',
    python_callable=create_orders_agg_table_py,
    dag=dag
)

clear_agg = PythonOperator(
    task_id = 'clear_orders_agg',
    python_callable=clear_orders_agg_py,
    dag=dag
)

create_click_agg = PythonOperator(
    task_id='create_click_orders_agg_table',
    python_callable=create_click_orders_agg_table_py,
    dag=dag
)

clear_click_orders_agg = PythonOperator(
    task_id = 'clear_click_orders_agg',
    python_callable=clear_click_orders_agg_py,
    dag=dag
)

copy_aggregates = PythonOperator(
    task_id = 'copy_aggregates',
    python_callable=copy_aggregates_py,
    dag=dag
)

loads = []

for region in ['North', 'South', 'East', 'West']:
    loads.append(PythonOperator(
        task_id=f'loads_{region}',
        python_callable=load_py,
        op_kwargs={'region': region},
        pool_slots=80,
        dag=dag
        )
    )

for i in range(len(loads)):
    clear_agg >> loads[i] >> create_click_agg

start >> generator >> create_agg >> clear_agg >> create_click_agg >> clear_click_orders_agg >> copy_aggregates >> finish