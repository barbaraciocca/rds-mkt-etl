from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.ingest_raw_task import pipeline_funnel_raw_to_clean, pipeline_metas_raw
from tasks.ingest_clean_task import pipeline_funnel_clean, pipeline_metas_clean
from tasks.ingest_delivery_task import run_sql, sql_daily_metrics, sql_top_campaigns_all, sql_top_campaigns_hr, sql_pivot_metas
from utils.data_contract import validate_data_contract


with DAG(
    dag_id="etl_email_marketing",
    start_date=datetime(2025, 11, 30),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "airflow"},
) as dag:
    with TaskGroup("raw_ingestion", tooltip="Ingest CSVs into RAW schema") as raw_group:
        raw_metas_funnel = PythonOperator(
            task_id="raw_funnel",
            python_callable=pipeline_funnel_raw_to_clean
        )

        raw_metas = PythonOperator(
            task_id="raw_metas",
            python_callable=pipeline_metas_raw
        )

    with TaskGroup("clean_processing", tooltip="Clean RAW data and write to CLEAN schema") as clean_group:
        clean_funnel = PythonOperator(
            task_id="clean_funnel",
            python_callable=pipeline_funnel_clean
        )

        clean_metas = PythonOperator(
            task_id="clean_metas",
            python_callable=pipeline_metas_clean
        )

    with TaskGroup("cata_contract_validator", tooltip="Valid the data contract") as data_contract:
        validate_funnel_task = PythonOperator(
            task_id="validate_clean_funnel",
            python_callable=validate_data_contract,
            provide_context=True,
        )
    with TaskGroup("delivery_tasks", tooltip="Load DELIVERY tables") as delivery_group:
        pivot_metas_task = PythonOperator(
            task_id="pivot_metas",
            python_callable=run_sql,
            op_kwargs={"sql": sql_pivot_metas}
        )

        daily_metrics_task = PythonOperator(
            task_id="daily_metrics",
            python_callable=run_sql,
            op_kwargs={"sql": sql_daily_metrics}
        )

        top_campaigns_all_task = PythonOperator(
            task_id="top_campaigns_all",
            python_callable=run_sql,
            op_kwargs={"sql": sql_top_campaigns_all}
        )

        top_campaigns_hr_task = PythonOperator(
            task_id="top_campaigns_hr",
            python_callable=run_sql,
            op_kwargs={"sql": sql_top_campaigns_hr}
        )

        pivot_metas_task >> [daily_metrics_task, top_campaigns_all_task, top_campaigns_hr_task]

    raw_group >> clean_group >> data_contract >> delivery_group