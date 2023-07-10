# Importando Modulos
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperators
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023,7,7),
    'email': ['pablio.martins.067@ufrn.edu.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('workFlow_eng_dados', 
    description="Trabalho final da disciplina de Engenharia de Dados",
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2023,7,7),
    default_view='graph',
    tags=['Eng. Dados', 'Trabalho Final', 'Pipeline'],
    catchup=False)

verifica_pasta = FileSensor(task_id='verifica_pasta', filepath='/opt/airflow/data', dag=dag)
