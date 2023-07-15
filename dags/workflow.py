# Importando Modulos
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.connection import Connection
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import re
from requests.auth import HTTPBasicAuth

URL_API_DATASUS = 'https://imunizacao-es.saude.gov.br/_search'
USER = 'imunizacao_public'
PASSWORD = 'qlto5t&7r_@+#Tlstigi'

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023,7,7),
}

dag = DAG('workFlow_eng_dados', 
    description="Trabalho final da disciplina de Engenharia de Dados",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023,7,7),
    default_view='graph',
    tags=['Eng. Dados', 'Trabalho Final', 'Pipeline'],
    catchup=False)

file_sensor = FileSensor(
    task_id='file_sensor',
    filepath='/opt/airflow/requests/requests.json',
    poke_interval=10,
    fs_conn_id='connect_file',
    dag=dag
)

def process_file():
    with open('/opt/airflow/requests/requests.json') as file:
        data = json.load(file)

    return data

get_body_requests = PythonOperator(
    task_id='get_body_requests',
    python_callable=process_file,
    provide_context = True,
    dag=dag
)

remove_requests_json = BashOperator(
    task_id='remove_requests_json',
    bash_command='rm /opt/airflow/requests/requests.json',
    dag=dag
)

def api_query_and_save(**context):
    context_task = context['ti'].xcom_pull(task_ids='get_body_requests')

    date_requests= context_task['date']
    uf_requests = context_task['uf']
    body = context_task['body']

    response = requests.post(URL_API_DATASUS+'?scroll=3m', auth=HTTPBasicAuth(USER, PASSWORD), json=body)
    data = response.json()
    
    files = []
    number_part = 0
    while data["hits"]["hits"] != []:
        path_file = f'/opt/airflow/temp/data_{uf_requests}_{date_requests}_part{number_part}.json'
        with open(path_file, 'w') as file:
            json.dump(data, file)
        files.append(path_file)

        number_part += 1
        body = {
            "scroll_id": str(data["_scroll_id"]),
            "scroll": "3m"
        }
        response = requests.post(URL_API_DATASUS+'/scroll', auth=HTTPBasicAuth(USER, PASSWORD), json=body)
        data = response.json()
    return files
    

api_query = PythonOperator(
    task_id='api_query',
    python_callable=api_query_and_save,
    op_kwargs={'uf': 'BA', 'date': '2023-06-29'},
    dag=dag
)

def transform_data_csv(**context):
    files = context['ti'].xcom_pull(task_ids='api_query')
    
    regular_expression = r'data_[A-Z]{2}_\d{4}-\d{2}-\d{2}'
    file_name = re.search(regular_expression, files[0]).group()
    path_file = f'/opt/airflow/data/{file_name}.csv'
    df = None
    
    first_file = True
    for file in files:
        with open(file, 'r') as fileJson:
            data = json.load(fileJson)

        df = pd.DataFrame(list(map(lambda x: x["_source"], data["hits"]["hits"])))
        if first_file:
            columns = df.columns
            df.to_csv(path_file, index=False, mode='w', header=True)
            first_file = False
        else:
            df[columns].to_csv(path_file, index=False, mode='a', header=False)
    
    return path_file  

transform_data_to_csv = PythonOperator(
    task_id='transform_data_to_csv',
    python_callable=transform_data_csv,
    provide_context=True,
    dag=dag
)

clear_temp = BashOperator(
    task_id='clear_temp',
    bash_command='rm {{ti.xcom_pull(task_ids="api_query") | join(" ")}}',
    dag=dag
)

def extract_info_to_csv(**context):
    path_file = context['ti'].xcom_pull(task_ids='transform_data_to_csv')
    print(path_file)
    
    df = pd.read_csv(path_file, low_memory=False)
    
    df_sex_group = df.groupby(['estabelecimento_municipio_nome', 'paciente_enumSexoBiologico']).size().unstack().reset_index()

    df_sex_group.columns = ['cidade', 'feminino', 'masculino']

    df_sex_group.to_csv(f'{path_file[0:-4]}_sex.csv', index=False)

    df_vaccine_group = df.groupby(['estabelecimento_municipio_nome', 'vacina_nome']).size().unstack().reset_index()
    
    df_vaccine_group.fillna(0, inplace=True)

    df_vaccine_group.to_csv(f'{path_file[0:-4]}_vaccine.csv', index=False)


extract_info = PythonOperator(
    task_id='extract_info',
    python_callable=extract_info_to_csv,
    provide_context=True,
    dag=dag
)

file_sensor >> get_body_requests 
get_body_requests >> remove_requests_json
get_body_requests >> api_query >> transform_data_to_csv >> [clear_temp, extract_info]