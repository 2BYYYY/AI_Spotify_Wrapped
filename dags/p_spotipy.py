import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from PROJECT_Spotipy.spoti_main import main
from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
"""
    Old Version:
        Legacy imports:
            from airflow.operators.python
            from airflow.operators.email import EmailOperator
        New Version (via: https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html):
            To maintain compatibility with existing DAGs, the [apache-airflow-providers-standard] package is installable on both Airflow 2.x and 3.x.
            Users upgrading from Airflow 2.x are encouraged to begin updating import paths and testing provider installation in advance of the upgrade.

        SMTP Connections:
            !!!IMPORTANT IN EMAILOPERATOR!!!
            https://airflow.apache.org/docs/apache-airflow-providers-smtp/stable/index.html

            Connections via Airflow ADMIN GUI:
            https://airflow.apache.org/docs/apache-airflow-providers-smtp/stable/connections/smtp.html

            Needed:
                - disable_tls
                - from_email
"""

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'spotify_wrapped',
    default_args=default_args,
    description='A simple DAG to generate Spotify Wrapped CSV',
    schedule=None,
)

# Define the task to generate the CSV
generate_csv_task = PythonOperator(
    task_id='main_call',
    python_callable=main,
    dag=dag,
)

# Define the task to send the email
send_email_task = EmailOperator(
    task_id='send_email',
    to=['tobyyjavelona@gmail.com'],
    subject='Spotify Wrapped 2025',
    html_content='Please find attached the Spotify Wrapped 2025 CSV file.',
    files=["wrapped-grind-2025.csv"], 
    conn_id='smtp',
    dag=dag,
)

# Set the task dependencies
generate_csv_task >> send_email_task