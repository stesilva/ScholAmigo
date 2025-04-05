from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
import os, re, tarfile
from kafka_producer import produce

@dag('stream', start_date=datetime(2025, 3 , 29),
        description='DAG for streaming data', tags=['stream_processing'],
        schedule=None, catchup=False)
def my_dag():
    
    @task
    def produce_kafka():
        produce()

    produce_kafka()

my_dag()
