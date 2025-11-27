# Airflow Shopping Pipeline Project

This project demonstrates an ETL pipeline using **Apache Airflow**, **Postgres**, **Spark**, and the **DummyJSON API**.  
It is designed to be easily reproducible and accessible for anyone who wants to run it locally with Docker and Astro.

---------------------

## Prerequisites

- **Docker**  
- **Astro CLI** (installation guide: [Astro CLI Install](https://www.astronomer.io/docs/astro/cli/install-cli))
You can check your Astro CLI version with:
astro version

---------------------

## Start the Project

To start the local Airflow environment:
astro dev start

## Airflow requires several connections to run the ETL pipeline.
Open a bash session inside the Airflow container:

astro dev bash


Then add the following connections:

1. PostgreSQL Connection
airflow connections add 'postgres_default' \
  --conn-type 'postgres' \
  --conn-host 'postgres' \
  --conn-login 'postgres' \
  --conn-password 'postgres' \
  --conn-schema 'postgres' \
  --conn-port '5432'

2. DummyJSON API Connection
airflow connections add 'dummyjson_api' \
  --conn-type 'http' \
  --conn-host 'https://dummyjson.com/' \
  --conn-extra '{"endpoint":"products","headers":{"Content-Type":"application/json","User-Agent":"Mozilla/5.0","Accept":"application/json"}}'

3. Spark Connection
airflow connections add spark_default \
  --conn-type spark \
  --conn-host 'local[]' \
  --conn-extra '{"master": "local[]", "spark-binary": "spark-submit"}'

---------------------

▶️ Run the DAG

Open the Airflow UI and trigger the DAG:

etl_shopping_pipeline_dag

This will execute the full ETL pipeline.

---------------------

✨ Developer Signature

Project developed by Karim GARGOURI.
https://www.linkedin.com/in/karim-gargouri/