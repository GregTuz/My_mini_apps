import sys
import requests
import datetime
import json
from airflow import DAG
import ast
import logging
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, BooleanType
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('logger_bobber')
logger.setLevel(logging.INFO)


api_key = "f0a842dad1cbdee745397d6c6546650f"
s = requests.session()
tickers_url = f'http://api.marketstack.com/v1/tickers?access_key={api_key}'


def get_tickers_data():
	response = s.get(tickers_url, params={'limit': 5})
	content = response.json()
	logger.info(f'Collected data - {content.get("data")}')

	return response.json().get('data')


def flatten_stock_data(json_data, output_file: str) -> str:
	json_data = ast.literal_eval(json_data)
	flat_list = []

	for stock in json_data:
		flat_stock = {
			'name': stock.get('name'),
			'symbol': stock.get('symbol'),
			'has_intraday': stock.get('has_intraday', False),
			'has_eod': stock.get('has_eod', False),
			'country': stock.get('country'),
			'stock_exchange_name': stock.get('stock_exchange', {}).get('name'),
			'stock_exchange_acronym': stock.get('stock_exchange', {}).get('acronym'),
			'stock_exchange_mic': stock.get('stock_exchange', {}).get('mic'),
			'stock_exchange_country': stock.get('stock_exchange', {}).get('country'),
			'stock_exchange_country_code': stock.get('stock_exchange', {}).get('country_code'),
			'stock_exchange_city': stock.get('stock_exchange', {}).get('city'),
			'stock_exchange_website': stock.get('stock_exchange', {}).get('website')
		}
		flat_list.append(flat_stock)

	with open(output_file, 'w', encoding="utf-8") as f:
		json.dump(flat_list, f)
	f.close()
	logger.info(f"JSON file saved to {output_file}")

	return output_file


def create_spark_session():
	spark = SparkSession \
		.builder \
		.appName("spark_test") \
		.master("local[*]") \
		.config("spark.jars", "/opt/jars/postgresql-42.7.5.jar") \
		.getOrCreate()

	return spark


def create_dataframe(**kwargs):
	spark = create_spark_session()
	schema = StructType([
		StructField("name", StringType(), True),
		StructField("symbol", StringType(), True),
		StructField("has_intraday", BooleanType(), True),
		StructField("has_eod", BooleanType(), True),
		StructField("country", StringType(), True),
		StructField("stock_exchange_name", StringType(), True),
		StructField("stock_exchange_acronym", StringType(), True),
		StructField("stock_exchange_mic", StringType(), True),
		StructField("stock_exchange_country", StringType(), True),
		StructField("stock_exchange_country_code", StringType(), True),
		StructField("stock_exchange_city", StringType(), True),
		StructField("stock_exchange_website", StringType(), True)
	])
	df = spark.read.schema(schema).json('./tickers.json')
	logger.info(f'Dataframe is - {df.show(truncate=False)}')
	df.write.format("jdbc") \
		.option('driver', 'org.postgresql.Driver') \
		.option("url",'jdbc:postgresql://postgresql/stonks') \
		.option("dbtable", 'tickers') \
		.mode('append') \
		.option("multiline", "true") \
		.option('user', 'admin') \
		.option('password', 'admin') \
		.save()


with DAG(
		'tickers_data_to_postgres',
		default_args={
			'owner': 'admin',
			'depends_on_past': False,
			'start_date': datetime.datetime(2025, 1, 29),
			'retries': 1,
			'retry_delay': datetime.timedelta(minutes=5),
		},
		schedule_interval=None,
		catchup=False,
) as dag:

	get_tickers_task = PythonOperator(
		task_id='get_tickers',
		python_callable=get_tickers_data,
	)

	flatten_stock_data = PythonOperator(
		task_id='write_flettened_data',
		python_callable=flatten_stock_data,
		op_args=["{{ task_instance.xcom_pull(task_ids='get_tickers') }}", './tickers.json'],
	)

	create_dataframe_task = PythonOperator(
		task_id='create_dataframe',
		python_callable=create_dataframe,
		provide_context=True,
	)

	get_tickers_task >> flatten_stock_data >> create_dataframe_task