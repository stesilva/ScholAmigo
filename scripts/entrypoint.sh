# AWS-specific vars (optional; prefer IAM roles)
export AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials
export AWS_CONFIG_FILE=~/.aws/config
# # Airflow AWS connection (alternative to env vars)
export AIRFLOW_CONN_AWS_DEFAULT=${AIRFLOW_CONN_AWS_DEFAULT}

# Core Airflow setup
airflow db upgrade
airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
airflow webserver
