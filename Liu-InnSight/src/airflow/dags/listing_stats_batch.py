# Python package for Airflow Dags
from airflow import DAG
from datetime import datetime
# Python package to execute commands on a remote host
from airflow.contrib.operators.ssh_operator import SSHOperator

# Define default DAG argument
default_args = {
    'owner': 'price-insight',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Create a DAG named 'listing_stats_batch' with the default arguments.
# This DAG is only run once.
dag = DAG('listing_stats_batch',
    default_args=default_args,
    schedule_interval='@once')

# Create a task named 'data_fetch'.
# Note that ssh_conn_id is the connection id from Airflow connections.
# This task runs 'data_fetch.sh' to download data into s3.
data_fetch_task = SSHOperator(
    ssh_conn_id='data_fetch_conn',
    task_id='data_fetch',
    command='cd ~/InnSight/data_fetch; ./data_fetch.sh all',
    dag=dag)

# Create a task named 'config_generation'.
# This task runs 's3_urls_generation.sh' to grab all the s3 urls containing
# downloaded data.
config_generation_task = SSHOperator(
    ssh_conn_id='spark_master_conn',
    task_id='config_generation',
    command='cd ~/InnSight/batch_processing; ./s3_urls_generation.sh all',
    dag=dag)

# Create a task named 'data_cleaning'.
# This task runs the 'data_cleaning_to_parquet_batch.py' Spark script that
# reads the data from s3, cleans it and writes it to Parquet.
# Note:
# 1) Spark executor memory is memory required for executing tasks plus
#    overhead memory.
# 2) Soark master IP hardcoded.
data_cleaning_task = SSHOperator(
    ssh_conn_id='spark_master_conn',
    task_id='data_cleaning',
    command='source ~/.profile; '
            'cd ~/InnSight/batch_processing; '
            '~/.local/bin/spark-submit '
            '--executor-memory 4G --master spark://ip-10-0-0-11.us-west-2.compute.internal:7077 '
            'data_cleaning_to_parquet_batch.py all',
    dag=dag)

# Create a task named 'stats_aggregation'.
# This task runs the 'metrics_calculatioon_batch.py' Spark script that
# calculates metrics from the cleaned data in Parquet.
stats_aggregation_task = SSHOperator(
    ssh_conn_id='spark_master_conn',
    task_id='stats_aggregation',
    command='source ~/.profile; '
            'cd ~/InnSight/batch_processing; '
            '~/.local/bin/spark-submit '
            '--executor-memory 4G --master spark://ip-10-0-0-11.us-west-2.compute.internal:7077 '
            'metrics_calculation_batch.py all',
    dag=dag)

# Create DAG dependencies
config_generation_task.set_upstream(data_fetch_task)
data_cleaning_task.set_upstream(config_generation_task)
stats_aggregation_task.set_upstream(data_cleaning_task)
