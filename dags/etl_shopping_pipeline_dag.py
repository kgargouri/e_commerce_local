from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from include.is_available.is_api_available import is_api_available_callable
from include.extract_to_csv.api_to_csv_callable import api_to_csv_callable
from include.is_available.is_spark_available import is_spark_available_callable
from include.is_available.is_db_available import is_db_available_callable
from include.postgres_jobs.prepare_tables import prepare_tables_callable
from include.postgres_jobs.load_csv_to_postgres import load_to_postgres_callable

@dag(
    dag_id="etl_shopping_pipeline_dag",
    start_date=datetime(2025, 11, 1),
    schedule="0 2 * * *",   # every day at 2am
    catchup=False,
    tags=["etl"]
)
def etl_shopping_pipeline_dag():

    # Check if the API is available
    is_api_available = PythonSensor(
        task_id="is_api_available",
        python_callable=is_api_available_callable,
        poke_interval=30,
        timeout=300,
    )

    # Extraction to CSV
    with TaskGroup(group_id="extract_to_csv") as extract_to_csv:
        products_to_csv = PythonOperator(
            task_id="products_to_csv",
            python_callable=api_to_csv_callable,
            op_args=["products", "{{ ds }}"]
        )    
        users_to_csv = PythonOperator(
            task_id="users_to_csv",
            python_callable=api_to_csv_callable,
            op_args=["users", "{{ ds }}"]
        )    
        carts_to_csv = PythonOperator(
            task_id="carts_to_csv",
            python_callable=api_to_csv_callable,
            op_args=["carts", "{{ ds }}"]
        )
    
    # Check if Spark is available
    is_spark_available = PythonSensor(
        task_id="is_spark_available",
        python_callable=is_spark_available_callable,
        poke_interval=30,
        timeout=300,
    )

    # Transformation with PySpark
    with TaskGroup(group_id="transform_with_spark") as transform_with_spark:
        spark_products = SparkSubmitOperator(
            task_id="spark_transform_products",
            application="/usr/local/airflow/include/spark_jobs/transform_products.py",
            conn_id="spark_default",
            verbose=True,
            application_args=["{{ ds }}"]
        )
        spark_users = SparkSubmitOperator(
            task_id="spark_transform_users",
            application="/usr/local/airflow/include/spark_jobs/transform_users.py",
            conn_id="spark_default",
            verbose=True,
            application_args=["{{ ds }}"]
        )
        spark_carts = SparkSubmitOperator(
            task_id="spark_transform_carts",
            application="/usr/local/airflow/include/spark_jobs/transform_carts.py",
            conn_id="spark_default",
            verbose=True,
            application_args=["{{ ds }}"]
        )

    # Check if the database is available
    is_db_available = PythonSensor(
        task_id="is_db_available",
        python_callable=is_db_available_callable,
        poke_interval=30,
        timeout=300,
    )

    # Table preparation
    prepare_tables = PythonOperator(
        task_id="prepare_tables",
        python_callable=prepare_tables_callable,
    )

    # Load the data into PostgreSQL
    with TaskGroup(group_id="load_to_postgres") as load_to_postgres:
        load_products = PythonOperator(
            task_id="load_products",
            python_callable=load_to_postgres_callable,
            op_args=["products", "{{ ds }}"]
        )
        load_users = PythonOperator(
            task_id="load_users",
            python_callable=load_to_postgres_callable,
            op_args=["users", "{{ ds }}"]
        )
        load_carts = PythonOperator(
            task_id="load_carts",
            python_callable=load_to_postgres_callable,
            op_args=["carts", "{{ ds }}"]
        )


    is_api_available >> extract_to_csv >> is_spark_available >> transform_with_spark >> is_db_available >> prepare_tables >> load_to_postgres

etl_shopping_pipeline_dag()