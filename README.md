# RDStation - Data Engineer - Email Marketing

This project builds an ETL pipeline for email marketing data using Airflow for orchestration and Postgres as a data warehouse, running in Docker. The pipeline ingests, cleans, validates, and delivers data.
 
To view the tables, use a database client like DBeaver.
To connect to the database insert the configurations below:
 ```sh
    port: 5433
    host: localhost
    username and password: postgres
    database: datawarehouse
```
Three final CSV tables are available in the `results_tables` folder.

Documentation for each table, separated by layer (raw, clean, delivery), is provided as YAML files in `plugins/documentation`. Note: these YAMLs are for documentation only and are not executed.

A data contract is defined and validated in the Airflow pipeline after the cleaning step.

## How to Run

0. Install Docker to run this repository (https://www.docker.com/get-started/)

1. Clone the repo:
    ```sh
    git clone https://github.com/barbaraciocca/rds-mkt-etl.git
    ```

2. Initialize services:
    ```sh
    make init
    make up
    ```

3. Access Airflow at [localhost:8080](http://localhost:8080)  
    - Username: `airflow`  
    - Password: `airflow`  
    - Turn on the DAG `etl_email_marketing` in the Airflow webserver.
