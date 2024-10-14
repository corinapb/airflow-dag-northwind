from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow import DAG

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'NorthwindELT',
    default_args=default_args,
    description='A ELT dag for the Northwind ECommerceData',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        ELT Diária do banco de dados de ecommerce Northwind,
        começando em 2022-02-07. 
    """

    extract_postgres_task = BashOperator(
        task_id='extract_postgres',
        bash_command='echo "Extracted!" ',
    )
    
    extract_postgres_task.doc_md = dedent(
        """\
    #### Task Documentation

    Essa task extrai os dados do banco de dados postgres, parte de baixo do step 1 da imagem:
 
    ![img](https://user-images.githubusercontent.com/49417424/105993225-e2aefb00-6084-11eb-96af-3ec3716b151a.png)

    """
    )

