from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from tasks.rakuya_step import extract, transform, load
#記得airflow的connect要先設定mongo的conn_id


with DAG(
    'houseprice',
    default_args={
        'depends_on_past': False, #每一次執行的Task是否會依賴於上次執行的Task，如果是False的話，代表上次的Task如果執行失敗，這次的Task就不會繼續執行
        'email': ['airflow@example.com'], #如果Task執行失敗的話，要寄信給哪些人的email
        'email_on_failure': False, #如果Task執行失敗的話，是否寄信
        'email_on_retry': False, #如果Task重試的話，是否寄信
        'retries': 1, #最多重試的次數
        'retry_delay': timedelta(minutes=5), #每次重試中間的間隔
    },
    description='houseprice king and rakuya crawler every to mongo',
    schedule_interval= '@daily' , #'*/3 * * * *', #timedelta(days=1),
    start_date=datetime(2022, 1, 1), #Task從哪個日起後，開始不被Scheduler放入排程
    catchup=False,
    tags=['houseprice'],
) as dag:

    c1 = BashOperator(
    task_id='c1',
    bash_command='python /Users/chunweichang/airflow/tasks/rakuya_request.py',
    )
    
    t1 = PythonOperator(
        task_id='t1',
        python_callable=extract,
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=transform,
    )

    t3 = PythonOperator(
        task_id='t3',
        python_callable=load,
    )

    c1 >> t1 >> t2 >> t3
 

