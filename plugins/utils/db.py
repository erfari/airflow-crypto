import csv
import os
import sys
from io import StringIO

from airflow.hooks.base import BaseHook
from sqlalchemy_utils.types.pg_composite import psycopg2

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


def _get_db_url(connector) -> str:
    connection = BaseHook.get_connection(connector)
    return (f'user={connection.login}'
            f' password={connection.password}'
            f' host={connection.host}'
            f' port={connection.port}'
            f' dbname={connection.dbname}')


def load_data_db(connector, df, table_name) -> None:
    buffer = StringIO()
    df.to_csv(buffer, index=False, sep=',', na_rep='NUL', quoting=csv.QUOTE_MINIMAL,
              header=False, float_format='%.8f', doublequote=False, escapechar='\\')
    buffer.seek(0)
    copy_query = f"""
        COPY {table_name}({'.'.join(df.columns)})
        FROM STDIN
        DELIMETER ','
        NULL 'NUL'
    """
    conn = psycopg2.connect(dsn=_get_db_url(connector))
    with conn.cursor() as cursor:
        cursor.copy_expert(copy_query, buffer)
        conn.commit()
        conn.close()
