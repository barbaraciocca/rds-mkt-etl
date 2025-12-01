import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from utils.params import CONN_ID, RAW_SCHEMA, CLEAN_SCHEMA, RAW_TABLE_METAS, CLEAN_TABLE_FUNNEL, RAW_FUNNEL_CSV, RAW_METAS_CSV, RAW_TABLE_FUNNEL, CLEAN_TABLE_METAS


def read_postgres(table: str, schema: str) -> pd.DataFrame:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    query = f"SELECT * FROM {schema}.{table}"
    return pd.read_sql(query, engine)

def write_postgres(df: pd.DataFrame, table: str, schema: str):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi"
    )

def clean_funnel_manual(df: pd.DataFrame) -> pd.DataFrame:
    if 'id' in df.columns:
        df = df[df['id'].notnull() & (df['id'] != "")]
        df = df.drop_duplicates(subset=['id'], keep='first')

    date_cols = [
        "created_month","lead_created_month",
        "mql_created_month","sal_created_month"
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    timestamp_cols = [
        "created_date","lead_created_date",
        "mql_created_date","sal_created_date",
        "first_touch_date","last_touch_date"
    ]
    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.tz_localize('UTC').dt.tz_convert('Etc/GMT+3').dt.tz_localize(None)

    string_cols = [
        "id","email","account_first_touch","account_last_touch",
        "channel_first_touch","channel_last_touch",
        "campaign_first_touch","campaign_last_touch",
        "identifier_first_touch","identifier_last_touch",
        "icp_score","lead_channel","lead_type","country"
    ]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    int_cols = [
        "qty_conversions"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')

    bool_cols = ["is_new_lead",
        "converted_to_mql",
        "is_blank_mql",
        "converted_to_sal",
        "is_a", "is_b", "is_c", "is_d", "is_abc"
    ]
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].astype(bool)

    return df

def clean_metas_manual(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()

    string_cols = ["conta","canal_mkt","etapa","perfil"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    int_cols = ["mes","meta"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')

    date_cols = ["data"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    return df

def pipeline_funnel_clean():
    df_raw = read_postgres(RAW_TABLE_FUNNEL, RAW_SCHEMA)
    df_clean = clean_funnel_manual(df_raw)
    write_postgres(df_clean, CLEAN_TABLE_FUNNEL, CLEAN_SCHEMA)

def pipeline_metas_clean():
    df_raw = read_postgres(RAW_TABLE_METAS, RAW_SCHEMA)
    df_clean = clean_metas_manual(df_raw)
    write_postgres(df_clean, CLEAN_TABLE_METAS, CLEAN_SCHEMA)
