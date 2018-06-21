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

dag = DAG('initialisation_impala', default_args=default_args, schedule_interval="*/3 * * * *", start_date=datetime.now() - timedelta(minutes=1))


echo = BashOperator(
    task_id='echo',
    bash_command="echo initialisation task ",
    dag=dag)

remove_from_hdfs = BashOperator(
    task_id='remove_from_hdfs',
    bash_command="hadoop fs -rm -r /user/cloudera/user; hadoop fs -rm -r /user/cloudera/activitylog",
    dag=dag)


initialise_script = BashOperator(
    task_id='initialise_script',
    bash_command="cd /home/cloudera/Documents/data/; ./initialise-impala.sh ",
    dag=dag)

echo.set_downstream(initialise_script)

