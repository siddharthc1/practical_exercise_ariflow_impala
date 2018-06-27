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

dag = DAG('airflow_task_script', default_args=default_args, schedule_interval=None, start_date=datetime.now() - timedelta(minutes=1))


generating_the_data= BashOperator(
    task_id='generating_the_data',
    bash_command="cd ~/Documents/data/ ; python practical_exercise_data_generator.py --load_data; python practical_exercise_data_generator.py --create_csv",
    dag=dag)

Sqoop_import_user= BashOperator(
    task_id='Sqoop_import_user',
    bash_command="  sqoop import --connect jdbc:mysql://localhost/practical_exercise_1 --username root --password-file /user/cloudera/root_pwd.txt --table user -m 4 --hive-import --hive-overwrite --hive-database practical_exercise_1 --hive-table user ",
    dag=dag)

Sqoop_import_activitylog= BashOperator(
    task_id='Sqoop_import_activitylog',
    bash_command=" sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --exec practical_exercise_1.activitylog;  ",
    dag=dag)

CSV_to_HDFS= BashOperator(
    task_id='CSV_to_HDFS',
    bash_command="hadoop fs -put ~/Documents/data/*.csv /user/cloudera/workshop/process/ ",
    dag=dag)

Store_in_archive= BashOperator(
    task_id='Store_in_archieve',
    bash_command="sudo mv ~/Documents/data/*.csv ~/Documents/data/archive/",
    dag=dag)

Drop_user_report_table= BashOperator(
    task_id='Drop_user_report_table',
    bash_command=""" impala-shell -q "drop table if exists practical_exercise_1.user_report;" """,
    dag=dag)

Create_user_report= BashOperator(
    task_id='Create_user_report',
    bash_command=""" impala-shell -q "create table practical_exercise_1.user_report(user_id bigint, total_updates bigint, total_inserts bigint, total_deletes bigint, last_activity_type string, is_active boolean, upload_count bigint);" """,
    dag=dag)

Insert_user_report= BashOperator(
    task_id='Insert_user_report',
    bash_command=""" NOW=$(date +%s); impala-shell -q "invalidate metadata practical_exercise_1.user;";
impala-shell -q "invalidate metadata practical_exercise_1.user_upload_dump;";
impala-shell -q "invalidate metadata practical_exercise_1.activitylog;"; 
impala-shell -q "select a.user_id,COALESCE(b.co,0) as total_updates,COALESCE(c.co,0) as total_inserts, COALESCE(d.co,0) as total_deletes, e.co as last_activity_type, COALESCE(f.co,FALSE) as is_active, COALESCE(g.co,0) as upload_count from (select id as user_id from practical_exercise_1.user group by id) as a left join (select user_id, count(user_id) as co from practical_exercise_1.activitylog where type='UPDATE' group by user_id) as b on a.user_id=b.user_id left join (select user_id, count(user_id) as co from practical_exercise_1.activitylog where type='INSERT' group by user_id) as c on a.user_id=c.user_id left join(select user_id, count(user_id) as co from practical_exercise_1.activitylog where type='DELETE' group by user_id) as d on a.user_id=d.user_id left join (SELECT a.user_id, a.type as co FROM practical_exercise_1.activitylog a INNER JOIN (SELECT user_id, MAX(\`timestamp\`) as ti FROM practical_exercise_1.activitylog GROUP BY user_id ) AS b ON a.user_id = b.user_id AND a.\`timestamp\` = b.ti) as e on a.user_id=e.user_id left join (select user_id, if(count(*) = 0, FALSE, TRUE) as co from practical_exercise_1.activitylog where \`timestamp\` > $NOW-172800 group by user_id) as f on a.user_id=f.user_id left join (select user_id, count(user_id) as co from practical_exercise_1.user_upload_dump group by user_id) as g on a.user_id=g.user_id;" """,
    dag=dag)

Insert_user_total= BashOperator(
    task_id='Insert_user_total',
    bash_command="""impala-shell -q "invalidate metadata practical_exercise_1.user;"; impala-shell -q "insert into practical_exercise_1.user_total select current_timestamp(), sub1.t , case when sub2.t1 is NULL then sub1.t when sub2.t1 is not NULL then sub1.t-sub2.t1 end from (select count(distinct id) as t from practical_exercise_1.user)sub1, (select max(total_users) t1 from practical_exercise_1.user_total) sub2;" """,
    dag=dag)


generating_the_data.set_downstream(Sqoop_import_user)
generating_the_data.set_downstream(Sqoop_import_activitylog)
generating_the_data.set_downstream(CSV_to_HDFS)

CSV_to_HDFS.set_downstream(Store_in_archive)

Sqoop_import_user.set_downstream(Drop_user_report_table)
Sqoop_import_activitylog.set_downstream(Drop_user_report_table)
CSV_to_HDFS.set_downstream(Drop_user_report_table)

Drop_user_report_table.set_downstream(Create_user_report)
Create_user_report.set_downstream(Insert_user_report)

Sqoop_import_user.set_downstream(Insert_user_total)
Sqoop_import_activitylog.set_downstream(Insert_user_total)
CSV_to_HDFS.set_downstream(Insert_user_total)






