CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS CLEAN;
CREATE SCHEMA IF NOT EXISTS DELIVERY;

CREATE TABLE IF NOT EXISTS raw.funnel_raw (
    id TEXT,
    email TEXT,
    created_month TEXT,
    created_date TEXT,
    lead_created_month TEXT,
    lead_created_date TEXT,
    mql_created_month TEXT,
    mql_created_date TEXT,
    sal_created_month TEXT,
    sal_created_date TEXT,
    first_touch_date TEXT,
    last_touch_date TEXT,
    account_first_touch TEXT,
    account_last_touch TEXT,
    channel_first_touch TEXT,
    channel_last_touch TEXT,
    campaign_first_touch TEXT,
    campaign_last_touch TEXT,
    identifier_first_touch TEXT,
    identifier_last_touch TEXT,
    icp_score TEXT,
    lead_channel TEXT,
    lead_type TEXT,
    country TEXT,
    qty_conversions TEXT,
    is_new_lead TEXT,
    converted_to_mql TEXT,
    is_blank_mql TEXT,
    converted_to_sal TEXT,
    is_a TEXT,
    is_b TEXT,
    is_c TEXT,
    is_d TEXT,
    is_abc TEXT
);

CREATE TABLE IF NOT EXISTS raw.metas_raw (
    conta TEXT,
    canal_mkt TEXT,
    etapa TEXT,
    perfil TEXT,
    mes TEXT,
    meta TEXT,
    data TEXT
);


CREATE TABLE IF NOT EXISTS clean.clean_funnel (
    id TEXT PRIMARY KEY,
    email TEXT,
    created_month DATE,
    created_date TIMESTAMP,
    lead_created_month DATE,
    lead_created_date TIMESTAMP,
    mql_created_month DATE,
    mql_created_date TIMESTAMP,
    sal_created_month DATE,
    sal_created_date TIMESTAMP,
    first_touch_date TIMESTAMP,
    last_touch_date TIMESTAMP,
    account_first_touch TEXT,
    account_last_touch TEXT,
    channel_first_touch TEXT,
    channel_last_touch TEXT,
    campaign_first_touch TEXT,
    campaign_last_touch TEXT,
    identifier_first_touch TEXT,
    identifier_last_touch TEXT,
    icp_score CHAR(1),
    lead_channel TEXT,
    lead_type TEXT,
    country TEXT,
    qty_conversions TEXT,
    is_new_lead BOOLEAN,
    converted_to_mql BOOLEAN,
    is_blank_mql BOOLEAN,
    converted_to_sal BOOLEAN,
    is_a BOOLEAN,
    is_b BOOLEAN,
    is_c BOOLEAN,
    is_d BOOLEAN,
    is_abc BOOLEAN
);

CREATE TABLE IF NOT EXISTS clean.clean_metas (
    conta TEXT,
    canal_mkt TEXT,
    etapa TEXT,
    perfil CHAR(1),
    mes INTEGER,
    meta INTEGER,
    data DATE
);

CREATE TABLE IF NOT EXISTS delivery.daily_metrics (
    data_referencia DATE,
    account TEXT,
    icp_score CHAR(1),
    tipo_lead VARCHAR(10),
    pais TEXT,
    leads INTEGER,
    emails_unicos INTEGER,
    mqls INTEGER,
    sals INTEGER,
    ultimo_horario_conversao TIME,
    meta_leads_diaria NUMERIC,
    meta_mql_diaria NUMERIC,
    meta_sal_diaria NUMERIC,
    taxa_lead_mql_pct NUMERIC,
    taxa_mql_sal_pct NUMERIC,
    leads_mtd INTEGER,
    mqls_mtd INTEGER,
    leads_vs_meta_mtd_pct NUMERIC
);

CREATE TABLE IF NOT EXISTS delivery.top_campaigns_all (
    campaign_last_touch TEXT,
    icp_score CHAR(1),
    leads INTEGER,
    mqls INTEGER,
    sals INTEGER,
    rank_leads INTEGER,
    rank_mqls INTEGER,
    rank_sals INTEGER
);

CREATE TABLE IF NOT EXISTS delivery.top_campaigns_hr (
    campaign_last_touch TEXT,
    leads_hr INTEGER
);