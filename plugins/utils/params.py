CONN_ID = "POSTGRES_DATAWAREHOUSE"

RAW_SCHEMA = "raw"
CLEAN_SCHEMA = "clean"
DELIVERY_SCHEMA = "delivery"

RAW_FUNNEL_CSV = "/opt/airflow/plugins/data/bi_challenge_rd_bi_funnel_email.csv"
RAW_METAS_CSV = "/opt/airflow/plugins/data/metas_email.csv"

RAW_TABLE_FUNNEL = "funnel_raw"
RAW_TABLE_METAS = "metas_raw"

CLEAN_TABLE_FUNNEL = "clean_funnel"
CLEAN_TABLE_METAS = "clean_metas"

DELIVERY_TABLE_DAILY = "daily_metrics"
DELIVERY_TABLE_TOPCAMPAIGNS = "top_campaigns_all"
DELIVERY_TABLE_TOPCAMPAIGNS_HR = "top_campaigns_hr"
PIVOT_TABLE = "pivot_daily_metrics"