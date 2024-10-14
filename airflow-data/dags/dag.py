from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import sqlite3
import pandas as pd

# 1. Crie uma task que lê os dados da tabela 'Order' do banco de dados disponível em data/Northwhind_small.sqlite. O formato do banco de dados é o Sqlite3. Essa task deve escrever um arquivo chamado "output_orders.csv".

# 1. Função que lê os dados da tabela 'Order' e exporta para "output_orders.csv".
def export_orders():
    conn = sqlite3.connect('/Users/corinabachmann/indicium/airflow_tooltorial/data/Northwind_small.sqlite')
    query = "SELECT * FROM 'Order'"
    df = pd.read_sql(query, conn)
    df.to_csv('/Users/corinabachmann/indicium/airflow_tooltorial/airflow-data/dags/output_orders.csv', index=False)
    conn.close()

# 2. Crie uma task que lê os dados da tabela "OrderDetail" do mesmo banco de dados e faz um JOIN com o arquivo "output_orders.csv" que você exportou na tarefa anterior. Essa task deve calcular qual a soma da quantidade vendida (Quantity) com destino (ShipCity) para o Rio de Janeiro. Você deve exportar essa contagem em arquivo "count.txt" que contenha somente esse valor em formato texto (use a função str() para converter número em texto). 

# 2. Função que calcula a soma da quantidade vendida para o Rio de Janeiro.
def calculate_quantity_for_rio():
    conn = sqlite3.connect('/Users/corinabachmann/indicium/airflow_tooltorial/data/Northwind_small.sqlite')
    query = "SELECT * FROM 'OrderDetail'"
    order_details = pd.read_sql(query, conn)
    orders = pd.read_csv('/Users/corinabachmann/indicium/airflow_tooltorial/airflow-data/dags/output_orders.csv')
    orders.rename(columns={'Id': 'OrderId'}, inplace=True)
    merged_data = order_details.merge(orders, on='OrderId')
    quantity_sum = merged_data[merged_data['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()
    with open('/Users/corinabachmann/indicium/airflow_tooltorial/airflow-data/dags/count.txt', 'w') as f:
      f.write(str(quantity_sum))

    conn.close()


# 3. Adicione uma variável no Airflow com a key "my_email" e no campo "value" adicione seu email @indicium.tech. 

# 4. Crie uma ordenação de execução das Tasks que deve terminar com a task export_final_output conforme o exemplo abaixo:
# task1 >> task2 >> export_final_output

# 5. Você deve conseguir rodar o DAG sem erros e gerar o arquivo final_output.txt com apenas um texto codificado gerado automaticamente pela task export_final_output.

# 3. Função que exporta o resultado final para "final_output.txt".
def export_final_output():
    with open('/Users/corinabachmann/indicium/airflow_tooltorial/airflow-data/dags/count.txt', 'r') as f:
        content = f.read()
    with open('/Users/corinabachmann/indicium/airflow_tooltorial/airflow-data/dags/final_output.txt', 'w') as f_out:
        f_out.write(content)

# 4. Definição do DAG.
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 10),
}

with DAG(
    'northwind_elt',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='export_orders',
        python_callable=export_orders
    )

    task2 = PythonOperator(
        task_id='calculate_quantity_for_rio',
        python_callable=calculate_quantity_for_rio
    )

    final_task = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_output
    )

    # Definindo a ordem de execução
    task1 >> task2 >> final_task