from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from utils.params import CONN_ID, CLEAN_SCHEMA, CLEAN_TABLE_FUNNEL

sql_validate_funnel = f"""
    WITH validation_results AS (
        SELECT
            SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id_count,
            SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email_count,
            SUM(CASE WHEN last_touch_date IS NULL THEN 1 ELSE 0 END) AS null_last_touch_date_count,
            SUM(CASE WHEN icp_score IS NOT NULL AND icp_score NOT IN ('A', 'B', 'C', 'D') THEN 1 ELSE 0 END) AS invalid_icp_score_count,
            (SELECT COUNT(id) - COUNT(DISTINCT id) FROM {CLEAN_SCHEMA}.{CLEAN_TABLE_FUNNEL}) AS duplicate_id_count
            
        FROM 
            {CLEAN_SCHEMA}.{CLEAN_TABLE_FUNNEL}
    )
    SELECT
        null_id_count,
        null_email_count,
        null_last_touch_date_count,
        invalid_icp_score_count,
        duplicate_id_count,
        CASE 
            WHEN (null_id_count + null_email_count + null_last_touch_date_count + invalid_icp_score_count + duplicate_id_count) > 0 
            THEN 'FAIL' 
            ELSE 'PASS' 
        END AS validation_status
    FROM validation_results;
"""

def validate_data_contract(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    result = hook.get_first(sql_validate_funnel)
    null_id, null_email, null_last_touch, invalid_icp, duplicate_id, status = result
    
    print(f"Data Contract Validation Results for {CLEAN_TABLE_FUNNEL}")
    print(f"Status: {status}")
    print(f"Null IDs: {null_id}")
    print(f"Null Emails: {null_email}")
    print(f"Null last_touch_date: {null_last_touch}")
    print(f"Invalid ICP Scores: {invalid_icp}")
    print(f"Duplicate IDs: {duplicate_id}")

    if status == 'FAIL':
        raise AirflowException(
            f"Data contract validation FAILED on {CLEAN_TABLE_FUNNEL}. Found {null_id} null IDs, {null_last_touch} null dates, and {duplicate_id} duplicates. Stopping pipeline."
        )
    return status
