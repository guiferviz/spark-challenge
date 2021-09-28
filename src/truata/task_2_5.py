from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "start_date": days_ago(5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


truata_airflow = DAG(
    "truata_airflow",
    default_args=default_args,
    description="Our first DAG",
    schedule_interval=timedelta(days=1),
)

task_1 = DummyOperator(
    task_id="task1",
    dag=truata_airflow,
)

task_2 = DummyOperator(
    task_id="task2",
    dag=truata_airflow,
)
task_1.set_downstream(task_2)

task_3 = DummyOperator(
    task_id="task3",
    dag=truata_airflow,
)
task_1.set_downstream(task_3)

task_4 = DummyOperator(
    task_id="task4",
    dag=truata_airflow,
)
task_2.set_downstream(task_4)
task_3.set_downstream(task_4)

task_5 = DummyOperator(
    task_id="task5",
    dag=truata_airflow,
)
task_2.set_downstream(task_5)
task_3.set_downstream(task_5)

task_6 = DummyOperator(
    task_id="task6",
    dag=truata_airflow,
)
task_2.set_downstream(task_6)
task_3.set_downstream(task_6)
