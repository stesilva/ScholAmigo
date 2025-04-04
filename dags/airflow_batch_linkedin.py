# from datetime import datetime
# from airflow.operators.python import PythonOperator
# from airflow import DAG
# from airflow.utils.helpers import chain
# from airflow.exceptions import AirflowException
# import os, re, tarfile
# from scraping_daad import scrape_callable, send_data_to_aws

# log_file='/Users/elnararb/airflow/the_logs/log.txt'
# output_path='/Users/elnararb/airflow/the_logs'

# with DAG('scrape_daad', start_date=datetime(2024, 11, 8),
#          description='DAG to extract DAAD data', tags=['batch_scholarships'],
#          schedule='@monthly', catchup=False):
#     task_1 = PythonOperator(task_id='scrape', python_callable=scrape_callable)
#     task_2 = PythonOperator(task_id='send_to_aws', python_callable=send_data_to_aws)
#     task_1 >> task_2

from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
import os, re, tarfile
from linkedin_scrapping import scrape_linkedin
from datetime import timedelta


log_file='/Users/elnararb/airflow/the_logs/log.txt'
output_path='/Users/elnararb/airflow/the_logs'

@dag('batch_linkedin', start_date=datetime(2025, 3 , 29),
        description='DAG to scrape user data', tags=['batch_linkedin'],
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