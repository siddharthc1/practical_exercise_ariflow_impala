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

dag = DAG('initialisation_script', default_args=default_args, schedule_interval=None, start_date=datetime.now() - timedelta(minutes=1))


Starting_Sqoop_Metajob = BashOperator(
    task_id='Starting_Sqoop_Metajob',
    bash_command="nohup sqoop metastore &",
    dag=dag)

Deleting_Creating_Directories = BashOperator(
    task_id='Deleting_Creating_Directories',
    bash_command="hadoop fs -rm -r /user/cloudera/workshop/; hadoop fs -mkdir /user/cloudera/workshop/; hadoop fs -mkdir /user/cloudera/workshop/process/; ",
    dag=dag)

Drop_Database = BashOperator(
    task_id='Drop_Database',
    bash_command=""" impala-shell -q "drop database if exists practical_exercise_1 cascade;" """,
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

Drop_Database.set_downstream(Create_Database)
Starting_Sqoop_Metajob.set_downstream(Sqoop_Job)
Create_Database.set_downstream(Sqoop_Job)

Deleting_Creating_Directories.set_downstream(External_table)
Create_Database.set_downstream(External_table)

Create_Database.set_downstream(Creating_table_user_total)

