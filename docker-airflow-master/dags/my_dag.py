from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from random import randint
from datetime import datetime

def _training_model():
	return randint(1,10)

def _choose_best_model(**context):
	accuracies = context['ti'].xcom_pull(task_ids=[
		'training_model_A',
		'training_model_B',
		'training_model_C'

	])
	best_accuracy = max(accuracies)

	if (best_accuracy > 8):
		return 'accurate'
	return 'inaccurate'

with DAG("my_dag", start_date=datetime(2021,1,1),
	schedule_interval="@daily", catchup=False) as dag:
	
	# training_model_A = PythonOperator(
	# 	task_id="training_model_A",
	# 	python_callable=_training_model
	# )

	# training_model_B = PythonOperator(
	# 	task_id="training_model_B",
	# 	python_callable=_training_model
	# )

	# training_model_C = PythonOperator(
	# 	task_id="training_model_C",
	# 	python_callable=_training_model
	# )

	# Better way
	training_model_tasks = [
        PythonOperator(
            task_id=f"training_model_{model_id}",
            python_callable=_training_model,
            op_kwargs={
                "model": model_id
            }
        ) for model_id in ['A', 'B', 'C']
    ]

	choose_best_model = BranchPythonOperator(
		task_id="choose_best_mode",
		python_callable=_choose_best_model,
		dag=dag,
		provide_context=True
	)

	accurate = BashOperator(
		task_id="accurate",
		bash_command="echo 'accurate'"
	)

	inaccurate = BashOperator(
		task_id="inaccurate",
		bash_command="echo 'inaccurate'"
	)

	# [training_model_A, training_model_B, training_model_C] >>  choose_best_model >> [accurate,inaccurate]
	# Improve way
	training_model_tasks >> choosing_best_model >> [accurate, inaccurate]