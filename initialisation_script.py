﻿from airflow.operators import BashOperator
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

dag = DAG('initialisation_script_1', default_args=default_args, schedule_interval=None, start_date=datetime.now() - timedelta(minutes=1))


Starting_Sqoop_Metajob = BashOperator(
    task_id='Starting_Sqoop_Metajob',
    bash_command=""" 
metastore_jobs=`ps -eaf|grep "Sqoop meta" | wc -l`
if [ $metastore_jobs -lt 2 ];then
	nohup sqoop metastore &	
fi
""",
    dag=dag)

Creating_Directories = BashOperator(
    task_id='Creating_Directories',
    bash_command=" hadoop fs -mkdir -p /user/cloudera/workshop/process/ ",
    dag=dag)

Create_Database = BashOperator(
    task_id='Create_Database',
    bash_command="""impala-shell -q "create database practical_exercise_1;" """,
    dag=dag)

Sqoop_Job= BashOperator(
    task_id='Sqoop_Job',
    bash_command="sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --create practical_exercise_1.activitylog -- import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/root_pwd.txt --table activitylog -m 4 --hive-import --hive-database practical_exercise_1 --hive-table activitylog --incremental append --check-column id --last-value 0 ",
    dag=dag)


External_table = BashOperator(
    task_id='External_table',
    bash_command="""hive -e "CREATE EXTERNAL TABLE practical_exercise_1.user_upload_dump ( user_id int, file_name STRING, timestamp int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/cloudera/workshop/process/' tblproperties ('skip.header.line.count'='1');" """ ,
    dag=dag)

Creating_table_user_total = BashOperator(
    task_id='Creating_table_user_total',
    bash_command="""impala-shell -q "create table if not exists practical_exercise_1.user_total(time_ran timestamp, total_users bigint, users_added bigint);" """,
    dag=dag)

Starting_Sqoop_Metajob.set_downstream(Sqoop_Job)
Creating_Directories.set_downstream(External_table)
Create_Database.set_downstream(External_table)
Create_Database.set_downstream(Sqoop_Job)
Create_Database.set_downstream(Creating_table_user_total)

