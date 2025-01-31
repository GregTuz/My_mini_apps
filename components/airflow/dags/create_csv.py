import datetime
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

conn = psycopg2.connect(dbname='stonks', user='admin', password='admin', host='host.docker.internal')
cursor = conn.cursor()

def query_db_and_create_csv():
	cursor.execute('''
	select 
		s.symbol,
		abs(avg(s.open - s.close))  as avg_price_change
		
	from stocks s
	
	where s.symbol in (select symbol from tickers where stock_exchange_city = 'NEW YORK')
	
	group by symbol
''')
	df = pd.DataFrame(cursor.fetchall(), columns=['symbol', 'avg_price_change'])
	file_path = '/opt/airflow/data_output/result.csv'
	df.to_csv(file_path, index=False)

	return file_path


with DAG(
		'db_to_csv_dag',
		default_args={
			'owner': 'admin',
			'depends_on_past': False,
			'start_date': datetime.datetime(2025, 1, 29),
			'retries': 1,
			'retry_delay': datetime.timedelta(minutes=5),
			'description': 'DAG to query DB and create a CSV file',
			'schedule_interval': None,
		},
		schedule_interval=None,
		catchup=False,
) as dag:

	create_csv_task = PythonOperator(
		task_id='query_and_create_csv',
		python_callable=query_db_and_create_csv,
	)

	create_csv_task
