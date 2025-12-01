from airflow.hooks.postgres_hook import PostgresHook
from utils.params import CONN_ID, CLEAN_SCHEMA, DELIVERY_SCHEMA, CLEAN_TABLE_FUNNEL, CLEAN_TABLE_METAS, DELIVERY_TABLE_DAILY, DELIVERY_TABLE_TOPCAMPAIGNS, DELIVERY_TABLE_TOPCAMPAIGNS_HR, PIVOT_TABLE

def run_sql(sql, **kwargs):
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(sql)

sql_pivot_metas = f"""
    DROP TABLE IF EXISTS {CLEAN_SCHEMA}.{PIVOT_TABLE};
    CREATE TABLE {CLEAN_SCHEMA}.{PIVOT_TABLE} AS
    SELECT
        conta,
        EXTRACT(YEAR FROM data) || '-' || LPAD(mes::TEXT, 2, '0') AS mes_ano,
        perfil AS icp_score,
        SUM(CASE WHEN etapa = 'LEAD' THEN meta ELSE 0 END) AS meta_leads,
        SUM(CASE WHEN etapa = 'MQL' THEN meta ELSE 0 END) AS meta_mql,
        SUM(CASE WHEN etapa = 'SAL' THEN meta ELSE 0 END) AS meta_sal
    FROM 
        {CLEAN_SCHEMA}.{CLEAN_TABLE_METAS}
    WHERE
        canal_mkt = 'Email'
    GROUP BY
        1, 2, 3;
"""
sql_daily_metrics = f"""
    INSERT INTO {DELIVERY_SCHEMA}.{DELIVERY_TABLE_DAILY} (
        data_referencia, account, icp_score, tipo_lead, pais, leads, emails_unicos, 
        mqls, sals, ultimo_horario_conversao, meta_leads_diaria, meta_mql_diaria, 
        meta_sal_diaria, taxa_lead_mql_pct, taxa_mql_sal_pct, leads_mtd, mqls_mtd, 
        leads_vs_meta_mtd_pct
    )
    WITH daily_aggregates AS (
        SELECT
            DATE(t1.last_touch_date) AS data_referencia,
            t1.account_last_touch AS account,
            t1.icp_score,
            LEFT(t1.lead_type, 10) AS tipo_lead,
            t1.country AS pais, 
            COUNT(t1.id) AS leads,
            SUM(CASE WHEN t1.converted_to_mql THEN 1 ELSE 0 END) AS mqls,
            SUM(CASE WHEN t1.converted_to_sal THEN 1 ELSE 0 END) AS sals, 
            COUNT(DISTINCT t1.email) AS emails_unicos,
            MAX(t1.last_touch_date::TIME) AS ultimo_horario_conversao
            
        FROM 
            {CLEAN_SCHEMA}.{CLEAN_TABLE_FUNNEL} t1
        WHERE
            t1.id IS NOT NULL 
            AND t1.last_touch_date IS NOT NULL
        GROUP BY
            1, 2, 3, 4, 5
    ),
    metrics_with_budget AS (
        SELECT
            da.data_referencia,
            EXTRACT(YEAR FROM da.data_referencia) || '-' || LPAD(EXTRACT(MONTH FROM da.data_referencia)::TEXT, 2, '0') AS mes_ano,
            da.account,
            da.icp_score,
            da.tipo_lead,
            da.pais,
            da.leads,
            da.mqls,
            da.sals,
            da.emails_unicos,
            da.ultimo_horario_conversao,
            (da.mqls::NUMERIC / NULLIF(da.leads, 0)) AS taxa_lead_mql_pct,
            (da.sals::NUMERIC / NULLIF(da.mqls, 0)) AS taxa_mql_sal_pct,
            COALESCE(m.meta_leads / 30.0, 0) AS meta_leads_diaria,
            COALESCE(m.meta_mql / 30.0, 0) AS meta_mql_diaria,
            COALESCE(m.meta_sal / 30.0, 0) AS meta_sal_diaria      
        FROM 
            daily_aggregates da
        LEFT JOIN 
            {CLEAN_SCHEMA}.{PIVOT_TABLE} m 
        ON 
            EXTRACT(YEAR FROM da.data_referencia) || '-' || LPAD(EXTRACT(MONTH FROM da.data_referencia)::TEXT, 2, '0') = m.mes_ano
            AND da.icp_score = m.icp_score
    ),
    metrics_with_mtd_actual_and_budget AS (
        SELECT
            *,
            SUM(leads) OVER (PARTITION BY mes_ano, account, icp_score, tipo_lead, pais ORDER BY data_referencia) AS leads_mtd,
            SUM(mqls) OVER (PARTITION BY mes_ano, account, icp_score, tipo_lead, pais ORDER BY data_referencia) AS mqls_mtd,
            SUM(meta_leads_diaria) OVER (PARTITION BY mes_ano, account, icp_score, tipo_lead, pais ORDER BY data_referencia) AS mtd_leads_budget
        FROM metrics_with_budget
    )
    SELECT
        mwb.data_referencia,
        mwb.account,
        mwb.icp_score,
        mwb.tipo_lead,
        mwb.pais,
        mwb.leads,
        mwb.emails_unicos,
        mwb.mqls,
        mwb.sals,
        mwb.ultimo_horario_conversao,
        mwb.meta_leads_diaria,
        mwb.meta_mql_diaria,
        mwb.meta_sal_diaria,
        mwb.taxa_lead_mql_pct,
        mwb.taxa_mql_sal_pct,
        mwb.leads_mtd,
        mwb.mqls_mtd,
        (mwb.leads_mtd::NUMERIC / NULLIF(mwb.mtd_leads_budget, 0)) AS leads_vs_meta_mtd_pct
    FROM 
        metrics_with_mtd_actual_and_budget mwb
    ORDER BY
        data_referencia, account, icp_score;
"""

sql_top_campaigns_all = f"""
    TRUNCATE TABLE {DELIVERY_SCHEMA}.{DELIVERY_TABLE_TOPCAMPAIGNS};

    INSERT INTO {DELIVERY_SCHEMA}.{DELIVERY_TABLE_TOPCAMPAIGNS} (
        campaign_last_touch, icp_score, leads, mqls, sals, rank_leads, rank_mqls, rank_sals
    )
    WITH campaign_metrics AS (
        SELECT
            t1.campaign_last_touch,
            t1.icp_score,
            COUNT(t1.id) AS leads_count,
            SUM(CASE WHEN t1.converted_to_mql THEN 1 ELSE 0 END) AS mqls_count,
            SUM(CASE WHEN t1.converted_to_sal THEN 1 ELSE 0 END) AS sals_count
        FROM 
            {CLEAN_SCHEMA}.{CLEAN_TABLE_FUNNEL} t1
        WHERE
            t1.campaign_last_touch IS NOT NULL
        GROUP BY
            1, 2
    )
    SELECT
        cm.campaign_last_touch,
        cm.icp_score,
        cm.leads_count AS leads,
        cm.mqls_count AS mqls,
        cm.sals_count AS sals,
        RANK() OVER (ORDER BY cm.leads_count DESC) AS rank_leads,
        RANK() OVER (ORDER BY cm.mqls_count DESC) AS rank_mqls,
        RANK() OVER (ORDER BY cm.sals_count DESC) AS rank_sals
    FROM 
        campaign_metrics cm
    ORDER BY leads DESC, mqls DESC, sals DESC
    LIMIT 5;
    """

sql_top_campaigns_hr = f"""
    TRUNCATE TABLE {DELIVERY_SCHEMA}.{DELIVERY_TABLE_TOPCAMPAIGNS_HR};

    INSERT INTO {DELIVERY_SCHEMA}.{DELIVERY_TABLE_TOPCAMPAIGNS_HR} (
        campaign_last_touch, leads_hr
    )
    SELECT
        t1.campaign_last_touch,
        COUNT(t1.id) AS leads_hr
    FROM 
        {CLEAN_SCHEMA}.{CLEAN_TABLE_FUNNEL} t1
    WHERE
        t1.lead_type = 'HR'
        AND t1.campaign_last_touch IS NOT NULL
    GROUP BY
        t1.campaign_last_touch
    ORDER BY
        leads_hr DESC
    LIMIT 5;
    """
