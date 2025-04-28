from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
import os, re, tarfile
from linkedin_generate_data import generate_linkedin
from datetime import timedelta


@dag('batch_linkedin', start_date=datetime(2025, 3 , 29),
        description='DAG to generate user data', tags=['batch_processing'],
        schedule='0 1 * * *', catchup=False)
def my_dag():

    @task
    def generate_linkedin_task(execution_timeout=timedelta(minutes=10)):
        generate_linkedin()

    generate_linkedin_task()

my_dag()
    
