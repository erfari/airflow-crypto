import os
from datetime import timedelta
from multiprocessing.connection import Client

from airflow import DAG
from airflow.utils.dates import days_ago
import sys

from utils import binance_data, db

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'airflow'
client = Client(os.getenv('BINANCE_TOKEN'), os.getenv('BINANCE_ID'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
        dag_id='load_btc_info',
        default_args=default_args,
        schedule_interval=None
) as dag:
    load_btc_data = 'load_btc_data',
    python_callable = db.load_data_db,
    op_kwargs = {
        'connector': CONN_ID,
        'df': binance_data.merge_data(
            binance_data.get_data_from_binance(client, 5, 'btcusdt'),
            binance_data.get_data_fears_and_greed()
        ),
        'table_name': 'btcusdt'
    }
