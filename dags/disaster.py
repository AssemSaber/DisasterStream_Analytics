from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'disaster_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule="0 23 * * *",
    catchup=False,
    tags=['Disaster']
) as dag:


    last_data_coming=BashOperator(
        task_id='last_data_coming',
        bash_command=""" dbt source freshness --profiles-dir /opt/airflow/dbt/Disaster/ --project-dir /opt/airflow/dbt/Disaster """
    )
    run_staging_models=BashOperator(
        task_id='run_staging',
        bash_command=""" dbt run --model staging.*  --profiles-dir /opt/airflow/dbt/Disaster/ --project-dir /opt/airflow/dbt/Disaster/ """
    ) 
    run_intermediate_models=BashOperator(
        task_id='run_intermediate_models',
        bash_command=""" dbt run --model intermediate.*  --profiles-dir /opt/airflow/dbt/Disaster/ --project-dir /opt/airflow/dbt/Disaster/ """
    ) 
    @task_group(group_id='disaster_mart')
    def disaster_mart():
        dim_disaster_type=BashOperator(
            task_id='dim_disaster_type',
            bash_command=""" dbt run --select dim_disaster_type  --profiles-dir /opt/airflow/dbt/Disaster/ --project-dir /opt/airflow/dbt/Disaster/ """
        )
        dim_location=BashOperator(
            task_id='dim_location',
            bash_command=""" dbt run --select dim_location  --profiles-dir /opt/airflow/dbt/Disaster/ --project-dir /opt/airflow/dbt/Disaster/ """
        )
        dim_team=BashOperator(
            task_id='dim_team',
            bash_command=""" dbt run --select dim_team  --profiles-dir /opt/airflow/dbt/Disaster/ --project-dir /opt/airflow/dbt/Disaster/ """
        )
        fact_disaster=BashOperator(
            task_id='fact_disaster',
            bash_command=""" dbt run --select fact_disaster  --profiles-dir /opt/airflow/dbt/Disaster/ --project-dir /opt/airflow/dbt/Disaster/ """
        )
        [dim_disaster_type,dim_location,dim_team]>>fact_disaster

    test_modeling=BashOperator(
        task_id='test-dimensional-modeling',
        bash_command= """ dbt test --select test_type:generic --profiles-dir /opt/airflow/dbt/Disaster/ --project-dir /opt/airflow/dbt/Disaster/ """
    )

    last_data_coming>>run_staging_models>>run_intermediate_models>>disaster_mart()>>test_modeling

