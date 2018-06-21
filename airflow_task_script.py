from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('airflow_task_script', default_args=default_args, schedule_interval="*/3 * * * *", start_date=datetime.now() - timedelta(minutes=1))


TASK4= BashOperator(
    task_id='TASK4',
    bash_command="cd /home/cloudera/Documents/data/; ./TASK-4.sh ",
    dag=dag)

TASK5= BashOperator(
    task_id='TASK5',
    bash_command=" cd /home/cloudera/Documents/data/; ./TASK-4.sh ",
    dag=dag)

TASK6= BashOperator(
    task_id='TASK6',
    bash_command="cd /home/cloudera/Documents/data/; ./TASK-4.sh ",
    dag=dag)



TASK4.set_downstream(TASK5)
TASK5.set_downstream(TASK6)

