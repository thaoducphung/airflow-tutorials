from airflow import DAG 
from datetime import datetime, timedelta 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator

from datacleaner import data_cleaner

default_args = {
	'owner':'Airflow',
	'start_date':datetime(2021,8,10),
	'retries':1,
	'retry_delay':timedelta(seconds=5)
}

dag = DAG('store_dag', 
	default_args=default_args, 
	schedule_interval='@daily',
	catchup=False,
	template_searchpath=['/usr/local/airflow/sql_files']
)

# Check if the source file is avaiable in the folder
# Use Bash file to check
t1 = BashOperator(
	task_id='check_file_exists',
	bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',
	retries=2,
	retry_delay=timedelta(seconds=15),
	dag=dag
)

# Task 2 is data cleanning, using Python Operator

t2 = PythonOperator(task_id='clean_raw_csv',
	python_callable=data_cleaner,
	dag=dag
)

# Task 3 run ddl command to transfer the clean data into MySQL file

t3 = MySqlOperator(
	task_id='create_mysql_table', 
	mysql_conn_id='mysql_conn',
	# Create sql script
	sql="create_table.sql",
	dag=dag
)

# Load the data file into table in MySQL
 
t4 = MySqlOperator(
	task_id='insert_mysql_table',
	mysql_conn_id='mysql_conn',
	# Load sql script
	sql="insert_into_table.sql",
	dag=dag
)
t1 >> t2 >> t3 >> t4
# t1.setdownstram(t2)

