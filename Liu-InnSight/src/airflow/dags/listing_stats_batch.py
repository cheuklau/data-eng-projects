from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'price-insight',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG('listing_stats_batch', default_args=default_args, schedule_interval='@once')

data_fetch_task = SSHOperator(
    ssh_conn_id='data_fetch_conn',
    task_id='data_fetch',
    command='cd ~/InnSight/data_fetch; ./data_fetch.sh all',
    dag=dag)

config_generation_task = SSHOperator(
    ssh_conn_id='spark_master_conn',
    task_id='config_generation',
    command='cd ~/InnSight/batch_processing; ./s3_urls_generation.sh all',
    dag=dag)

data_cleaning_task = SSHOperator(
    ssh_conn_id='spark_master_conn',
    task_id='data_cleaning',
    command='source ~/.profile; '
            'cd ~/InnSight/batch_processing; '
            '~/.local/bin/spark-submit '
            '--executor-memory 4G --master spark://ip-10-0-0-11.us-west-2.compute.internal:7077 '
            'data_cleaning_to_parquet_batch.py all',
    dag=dag)

stats_aggregation_task = SSHOperator(
    ssh_conn_id='spark_master_conn',
    task_id='stats_aggregation',
    command='source ~/.profile; '
            'cd ~/InnSight/batch_processing; '
            '~/.local/bin/spark-submit '
            '--executor-memory 4G --master spark://ip-10-0-0-11.us-west-2.compute.internal:7077 '
            'metrics_calculation_batch.py all',
    dag=dag)

config_generation_task.set_upstream(data_fetch_task)
data_cleaning_task.set_upstream(config_generation_task)
stats_aggregation_task.set_upstream(data_cleaning_task)
