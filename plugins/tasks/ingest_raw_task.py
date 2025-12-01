import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from utils.params import CONN_ID, RAW_SCHEMA, CLEAN_SCHEMA, RAW_TABLE_METAS, CLEAN_TABLE_FUNNEL, RAW_FUNNEL_CSV, RAW_METAS_CSV, RAW_TABLE_FUNNEL

def load_funnel_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_FUNNEL_CSV)
    return df

def load_metas_raw() -> pd.DataFrame:
    df = pd.read_csv(RAW_METAS_CSV)
    return df

def clean_funnel(df: pd.DataFrame) -> pd.DataFrame:
    if "id" in df.columns:
        df = df[df["id"].notnull() & (df["id"] != "")]
        df = df.drop_duplicates(subset=["id"], keep="first")

    for col in df.columns:
        if "created" in col or "date" in col or "timestamp" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].dt.tz_localize("UTC").dt.tz_convert("Etc/GMT+3")

    bool_cols = [c for c in df.columns if c.startswith("converted_to_") or c.startswith("is_")]
    for c in bool_cols:
        df[c] = df[c].astype(bool)

    return df

def clean_metas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = pd.to_numeric(df[col], errors="ignore")
    return df

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

def pipeline_funnel_raw_to_clean():
    df = load_funnel_raw()
    write_postgres(df, RAW_TABLE_FUNNEL, RAW_SCHEMA)

    df_clean = clean_funnel(df)
    write_postgres(df_clean, CLEAN_TABLE_FUNNEL, CLEAN_SCHEMA)

def pipeline_metas_raw():
    df = load_metas_raw()
    df_clean = clean_metas(df)
    write_postgres(df, RAW_TABLE_METAS, RAW_SCHEMA)
