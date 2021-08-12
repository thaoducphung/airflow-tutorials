from airflow import DAG 
from datetime import datetime, timedelta 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

from datacleaner import data_cleaner

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
	'owner':'Airflow',
	'start_date':datetime(2021,8,10),
	'retries':1,
	'retry_delay':timedelta(seconds=5)
}

with DAG('store_dag_advanced', 
	default_args=default_args, 
	schedule_interval='@daily',
	catchup=False,
	template_searchpath=['/usr/local/airflow/sql_files']
) as dag:

	# Check if the source file is avaiable in the folder
	# Use Bash file to check
	t1 = BashOperator(
		task_id='check_file_exists',
		bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
		retries=2,
		retry_delay=timedelta(seconds=15),
	)

	# Task 2 is data cleanning, using Python Operator

	t2 = PythonOperator(task_id='clean_raw_csv',
		python_callable=data_cleaner,
	)

	# Task 3 run ddl command to transfer the clean data into MySQL file

	t3 = MySqlOperator(
		task_id='create_mysql_table', 
		mysql_conn_id='mysql_conn',
		# Create sql script
		sql="create_table.sql",
	)

	# Task 4
	# Load the data file into table in MySQL
	 
	t4 = MySqlOperator(
		task_id='insert_mysql_table',
		mysql_conn_id='mysql_conn',
		# Load sql script
		sql="insert_into_table.sql",
	)

	# Task 5
	# Compute the file and export into reports given csv file using MySQL
	t5 = MySqlOperator(
		task_id='select_from_table',
		mysql_conn_id="mysql_conn",
		sql="select_from_table.sql",
	)

	# Task 6
	# Using bash operator to rename the file

	t6 = BashOperator(
		task_id='move_file1', 
		bash_command='cat ~/store_files_airflow/location_wise_profit.csv && mv ~/store_files_airflow/location_wise_profit.csv ~/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date, 
	)

	t7 = BashOperator(
		task_id='move_file2',
		bash_command='cat ~/store_files_airflow/store_wise_profit.csv && mv ~/store_files_airflow/store_wise_profit.csv ~/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date,
	)

	# Task 8 
	# Report email

	t8 = EmailOperator(
		task_id='send_email',
		to='thaophung070896@gmail.com',
		subject='Daily Report Generated',
		html_content="""<h1>Congratutations! Your store reports are ready. </h1>""",
		files=[
			'/usr/local/airflow/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date,
			'/usr/local/airflow/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date
		],
		# cc=,
		# bcc=,
	)

	# Task 9
	# Renaming the task of input file

	t9 = BashOperator(
		task_id='rename_raw',
		bash_command='mv ~/store_files_airflow/raw_store_transactions.csv ~/store_files_airflow/raw_store_transactions_%s.csv' % yesterday_date,
	)

	t1 >> t2 >> t3 >> t4 >> t5 >> [t6, t7] >> t8 >> t9
	# t1.setdownstram(t2)

