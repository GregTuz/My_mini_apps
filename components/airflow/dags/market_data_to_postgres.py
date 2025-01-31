import sys
import requests
import datetime
import json
from airflow import DAG
import logging
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, DoubleType, DecimalType, StringType, StructType, TimestampType


logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('logger_bobber')
logger.setLevel(logging.INFO)


api_key = "f0a842dad1cbdee745397d6c6546650f"
s = requests.session()
tickers_url = f'http://api.marketstack.com/v1/tickers?access_key={api_key}'
data_url = f'http://api.marketstack.com/v1/eod?access_key={api_key}'

def get_tickers():
	tickers = set()
	response = s.get(tickers_url, params={'limit': 10})
	content = response.json()
	for item in content.get('data'):
		tickers.add(item.get('symbol'))
	logger.info(f'Collected tickers - {tickers}')

	return tickers


def get_data(tickers):
	url_params = {
		'date_from': '2024-09-01',
		'date_to': '2024-09-30',
		# "symbols": ','.join(tickers),
		"symbols": 'MSFT,AAPL,AMZN,BABA',
		'limit': 10
	}
	response = s.get(data_url, params=url_params)
	logger.warning(f'Response code is {response} Collected data - {json.dumps(response.json())}')
	with open('/opt/airflow/dags/market_data.json', 'w') as f:
		json.dump(response.json().get('data'), f)

	return '/opt/airflow/dags/market_data.json'


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
		StructField("open", DoubleType(), True),
		StructField("high", DoubleType(), True),
		StructField("low", DoubleType(), True),
		StructField("close", DoubleType(), True),
		StructField("volume", DoubleType(), True),
		StructField("adj_high", DoubleType(), True),
		StructField("adj_low", DoubleType(), True),
		StructField("adj_close", DoubleType(), True),
		StructField("adj_open", DoubleType(), True),
		StructField("adj_volume", DoubleType(), True),
		StructField("split_factor", DecimalType(10, 2), True),
		StructField("dividend", DecimalType(10, 2), True),
		StructField("symbol", StringType(), False),
		StructField("exchange", StringType(), True),
		StructField("date", TimestampType(), True)
	])
	df = spark.read.schema(schema).json('/opt/airflow/dags/market_data.json')
	df.write.format("jdbc") \
		.option('driver', 'org.postgresql.Driver') \
		.option("url",'jdbc:postgresql://postgresql/stonks') \
		.option("dbtable", 'stocks') \
		.mode('append') \
		.option('user', 'admin') \
		.option('password', 'admin') \
		.save()


with DAG(
		'market_data_to_postgres',
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
		python_callable=get_tickers,
	)

	get_data_task = PythonOperator(
		task_id='get_data',
		python_callable=get_data,
		op_args=["{{ task_instance.xcom_pull(task_ids='get_tickers') }}"],
	)

	create_dataframe_task = PythonOperator(
		task_id='create_dataframe',
		python_callable=create_dataframe,
		provide_context=True,
	)

	get_tickers_task >> get_data_task >> create_dataframe_task