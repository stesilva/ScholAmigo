from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
import os, re, tarfile
from linkedin_scrapping import scrape_linkedin
from datetime import timedelta


@dag('batch_linkedin', start_date=datetime(2025, 3 , 29),
        description='DAG to scrape user data', tags=['batch_processing'],
        schedule='@daily', catchup=False)
def my_dag():

    @task
    def scrape_linkedin_task(execution_timeout=timedelta(minutes=10)):
        scrape_linkedin()

    scrape_linkedin_task()

my_dag()
    


    # @task
    # def cleanup_tempfile(file_path):
    #     import os
    #     if os.path.exists(file_path):
    #         os.remove(file_path)
    # aws_task(scholarship_data)
    # cleanup_tempfile(scholarship_data)