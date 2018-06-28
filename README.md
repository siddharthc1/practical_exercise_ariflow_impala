**DESCRIPTION**

In this project, two tables are improted from MySQL database to Hive and one type of CSV file is uploaded into HDFS. Reporting tables user_report and user_total are created using the tables improted to Hive and an External created from the CSV files and it is scheduled using Apache Airflow.


**SETTING THE ENVIRONMENT AND USAGE**

1. Clone the "practical_exercise_ariflow_impala" project from the git repo https://github.com/siddharthc1

2. Create a MySQL database with name practical_exercise_1

    ```
    mysql -u root -p
    mysql> create database practical_exercise_1; 
    ```
    
3. Install dependent python packages using PIP:
    
    ```
    pip install peewee==2.8.8
    pip install PyMySQL==0.7.10
    ```
        

4. Run the start.sh to start airflow deamons

    ```
    ./initialise.sh
    ```
    
5. Start the initialisation_script_1 dag on airflow.
    

6. Now start airflow_task_script_1 dag on airflow.






