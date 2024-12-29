import datetime
import os
import sys
from multiprocessing.connection import Client

import pandas as pd
import requests
from pandas import DataFrame

FEAR_AND_GREED = os.getenv('FEAR_AND_GREED')
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


def get_data_from_binance(client: Client, days_count: int, ticker: str) -> pd.DataFrame:
    until_this_date = datetime.datetime.now()
    since_this_date = until_this_date - datetime.timedelta(days=days_count)
    data = client.get_historical_klines(symbol=ticker,
                                        interval=client.KLINE_INTERVAL_1MINUTE,
                                        start_str=str(since_this_date),
                                        end_str=str(until_this_date))
    data = pd.DataFrame(data,
                        columns=['date_time', 'open', 'high', 'low', 'close', 'volume_btc', 'close_time', 'volume_usdt',
                                 'traders_count', 'drop1', 'drop2', 'drop3'])
    data.date_time = pd.to_datetime(data.date_time, unit='ms').dt.strftime('%Y-%m-%d %H-%M-%S')
    data.close_time = pd.to_datetime(data.close_time, unit='ms').dt.strftime('%Y-%m-%d %H-%M-%S')
    data = data.drop(['drop1', 'drop2', 'drop3'], axis=1)
    return data


def get_data_fears_and_greed() -> pd.DataFrame:
    r = requests.get(FEAR_AND_GREED)
    fear_and_greed = pd.DataFrame(r.json()['data'])
    fear_and_greed.timestamp = pd.to_datetime(fear_and_greed.timestamp, unit='s').dt.strftime('%Y-%m-%d %H-%M-%S')
    fear_and_greed.value = pd.to_numeric(fear_and_greed.value)
    fear_and_greed.value_classification = fear_and_greed.value_classification.str
    return fear_and_greed


def merge_data(binance_data: DataFrame, fear_and_greed_data: DataFrame) -> pd.DataFrame:
    data_merge = binance_data.merge(fear_and_greed_data, left_on='date_time', right_on='timestamp', how='left')
    data_merge.value = data_merge.value.bfill().ffill()
    data_merge.value_classification = data_merge.value_classification.bfill().ffill()
    data_merge = data_merge.drop(['timestamp', 'time_until_update'], axis=1)
    return data_merge
