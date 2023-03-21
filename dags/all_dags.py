from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
import json
import mysql.connector
import requests



mydb = mysql.connector.connect(
  host="http://localhost:3306/",
  user="username",
  password="my-secret-pw",
  database="some-mysql"
)

mycursor = mydb.cursor()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 18, 10, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#!country_dag
dag = DAG(
    'country_dag',
    default_args=default_args,
    description='A DAG to load country data from json to MySQL',
    schedule_interval=timedelta(days=1),
)

def start_dag():
    print("DAG is starting")

def read_country_data():
    with open('/path/to/country_name.json', 'r') as f:
        data = json.load(f)
        return data

def insert_country_data():
    country_data = read_country_data()
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    for country in country_data:
        cursor.execute(f"INSERT INTO country (country_name) VALUES ('{country}')")
    conn.commit()

def end_dag():
    print("DAG is complete")

start_task = PythonOperator(
    task_id='start',
    python_callable=start_dag,
    dag=dag,
)

read_task = PythonOperator(
    task_id='read_country_data',
    python_callable=read_country_data,
    dag=dag,
)

insert_task = PythonOperator(
    task_id='insert_country_data',
    python_callable=insert_country_data,
    dag=dag,
)

end_task = PythonOperator(
    task_id='end',
    python_callable=end_dag,
    dag=dag,
)

start_task >> read_task >> insert_task >> end_task

#!currency_dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 18, 10, 5, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_started():
    print('DAG started running at: ' + str(datetime.now()))

def fetch_currency():
    url = 'country_currency.json'
    response = requests.get(url)
    currency_data = json.loads(response.text)
    return currency_data

def insert_currency():
    currency_data = fetch_currency()
    mydb = mysql.connector.connect(
        host="http://localhost:3306/",
        user="username",
        password="my-secret-pw",
        database="some-mysql"
    )
    mycursor = mydb.cursor()
    for item in currency_data:
        sql = "INSERT INTO currency (country_code, currency) VALUES (%s, %s)"
        val = (item["country_code"], item["currency"])
        mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "rows inserted.")

def log_finished():
    print('DAG finished running at: ' + str(datetime.now()))

with DAG(
    'currency',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='log_started',
        python_callable=log_started
    )

    t2 = PythonOperator(
        task_id='fetch_currency',
        python_callable=fetch_currency
    )

    t3 = PythonOperator(
        task_id='insert_currency',
        python_callable=insert_currency
    )

    t4 = PythonOperator(
        task_id='log_finished',
        python_callable=log_finished
    )

    t1 >> t2 >> t3 >> t4





#!data_merge
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_merge',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

start_task = BashOperator(
    task_id='start_task',
    bash_command='echo "DAG started"',
    dag=dag,
)

sql_query = """
TRUNCATE TABLE data_merge;
INSERT INTO data_merge (col1, col2, col3, col4)
SELECT c.col1, c.col2, co.col3, cu.col4
FROM country co
JOIN currency cu ON co.country_id = cu.country_id
JOIN common c ON co.common_id = c.common_id;
"""

merge_tables = MySqlOperator(
    task_id='merge_tables',
    mysql_conn_id='mysql_conn',
    sql=sql_query,
    dag=dag,
)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "DAG finished"',
    dag=dag,
)

start_task >> merge_tables >> end_task
