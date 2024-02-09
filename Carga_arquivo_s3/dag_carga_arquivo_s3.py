from datetime import datetime, timedelta  
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # Exemplo: Inicia em 09/02/2024
    'start_date': datetime(2024, 2, 9),
    'email': ['carlos.dantas@funcionalcorp.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    # Em caso de erros, tente rodar novamente apenas 1 vez
    'retries': 1,
    # Tente novamente após 30 segundos depois do erro
    'retry_delay': timedelta(seconds=30),
    # Execute todo dia as 8
    'schedule_interval': '10 8 * * *'
}

with DAG(    
    dag_id='bcare_Arquivos_s3',
    default_args=default_args,
    schedule_interval='10 8 * * *',
    tags=['BCARE', 'Carga', 'Arquivos_s3'],
) as dag:    

    # Vamos Definir a nossa Primeira Tarefa 
    t0 = BashOperator(bash_command="cd /scripts/Carga_arquivo_s3", task_id="entrarNoDiretorio")
    t1 = BashOperator(bash_command="export PYTHONWARNINGS=\"ignore\"", task_id="ignoraWarningsPython")
    t2 = BashOperator(bash_command="export PYTHONIOENCODING=utf8", task_id="encodingUTF8")
    t3 = BashOperator(bash_command="source /airflow/venv/bin/activate", task_id="ativaVenv")
    t4 = BashOperator(bash_command="python3 /scripts/Carga_arquivo_s3/carga_arquivo_s3.py", task_id="CargaArquivo_S3")
    #t5 = BashOperator(bash_command="python3 /scripts/Carga_Serpro/enviaemail.py", task_id="EnviaEmail")
    
    t0 >> t1 >> t2 >> t3 >> t4 
