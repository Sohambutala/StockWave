from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import yfinance as yf
from kafka import KafkaProducer
import time
import json
import logging

default_args = {
    'owner': 'Soham',
    'start_date': datetime(2023, 11, 30, 00, 00)
}

def get_data(stock_name: str):
    # Ticker object for Stock
    stck = yf.Ticker(stock_name)

    # Dictionary to store data
    stock_data = {}

    # Stock info
    stock_data['stock_info'] = stck.info

    # Historical market data for the last 1 month
    stock_data['historical_data'] = stck.history(period="1d", interval="1m").to_dict(orient='records')

    # News
    stock_data['news'] = stck.news

    return stock_data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    curr_time = time.time()
    try:
        res = get_data('MSFT')
        producer.send('stock_data', json.dumps(res).encode('utf-8'))
    except Exception as e:
        logging.error(f'An error occured: {e}')

# with DAG('user_automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id = 'stream_data_from_api',
#         python_callable=get_data
#     )

stream_data()