from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
from alert import slack_alert

args = {
    'owner': 'ekoteguh',
    'start_date': datetime(2022, 1, 10),
    'schedule_interval': '@daily',
    'on_success_callback': slack_alert.task_send_success_slack_alert
}

DAG_ID = 'dag_choose_best_model'

def _choose_best_model():
    accuracy = 6
    if accuracy > 5:
        return 'accurate'
    else:
        return 'inaccurate'

with DAG(
    dag_id=DAG_ID,
    default_args = args,
    description='Choose Best Model',
    tags=['homework'],
    catchup=False
    ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    start >> choose_best_model >> [accurate, inaccurate]