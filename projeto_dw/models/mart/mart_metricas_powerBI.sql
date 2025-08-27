{{
    config(
        materialized = 'table',
        unique_key = 'sk_cliente',
        tags = ['mart', 'metrics']
    )
}}

with 
dim_clientes as (
    select * from {{ ref('int_dim_clientes') }}
),

fact_pedidos as (
    select 
        fp.*,
        cast(fp.dt_pedido as date) as data_pedido
    from {{ ref('int_fact_pedidos') }} fp
),

dim_date as (
    select * from {{ ref('int_dim_date') }}
),

pedidos_por_cliente as (
    select
        dc.sk_cliente,
        dc.cpf,
        dc.nome,
        dc.estado,
        dc.cidade,
        
        count(distinct fp.sk_pedido) as total_pedidos,
        sum(fp.valor_total_pedido) as valor_total_gasto,
        avg(fp.valor_total_pedido) as ticket_medio,
        
        min(fp.dt_pedido) as data_primeiro_pedido,
        max(fp.dt_pedido) as data_ultimo_pedido,
        
        min(dd.year_number) as primeiro_ano_compra,
        max(dd.year_number) as ultimo_ano_compra,
        count(distinct dd.year_number) as total_anos_ativos,
        
        (current_date - max(fp.dt_pedido)::date) as dias_desde_ultimo_pedido,
        
        case 
            when count(fp.sk_pedido) > 1 
            then (max(fp.dt_pedido)::date - min(fp.dt_pedido)::date)::float / nullif(count(fp.sk_pedido)-1,0)
            else null 
        end as frequencia_media_dias,
        
        case
            when count(distinct to_char(fp.dt_pedido,'YYYY-MM')) > 0 
            then sum(fp.valor_total_pedido) / count(distinct to_char(fp.dt_pedido,'YYYY-MM'))
            else 0 
        end as valor_medio_por_mes,
        
        case
            when count(distinct to_char(fp.dt_pedido,'YYYY-MM')) > 0 
            then count(fp.sk_pedido)::float / count(distinct to_char(fp.dt_pedido,'YYYY-MM'))
            else 0 
        end as frequencia_media_mensal
        
    from dim_clientes dc
    left join fact_pedidos fp on dc.sk_cliente = fp.fk_cliente
    left join dim_date dd on date_trunc('day', fp.dt_pedido) = dd.date_day
    group by 1,2,3,4,5
)

select 
    pc.*,

    case
        when valor_total_gasto is null or valor_total_gasto = 0 then 'Inativo'
        when valor_total_gasto > 5000 and dias_desde_ultimo_pedido <= 30 and frequencia_media_mensal >= 2 then 'Campeão'
        when valor_total_gasto > 3000 and dias_desde_ultimo_pedido <= 60 then 'Cliente Fiel'
        when valor_total_gasto > 1000 and dias_desde_ultimo_pedido <= 90 then 'Potencial'
        when valor_total_gasto > 0 and dias_desde_ultimo_pedido > 180 then 'Em Risco de Churn'
        else 'Em Observação'
    end as segmento_rfm,

    case 
        when valor_total_gasto is null or valor_total_gasto = 0 then 1
        when valor_total_gasto > 5000 then 5
        when valor_total_gasto > 3000 then 4
        when valor_total_gasto > 1000 then 3
        when valor_total_gasto > 0 then 2
        else 1
    end as score_valor,
    
    case 
        when dias_desde_ultimo_pedido is null then 1
        when dias_desde_ultimo_pedido <= 30 then 5
        when dias_desde_ultimo_pedido <= 60 then 4
        when dias_desde_ultimo_pedido <= 90 then 3
        when dias_desde_ultimo_pedido <= 180 then 2
        else 1
    end as score_recencia,
    
    case 
        when frequencia_media_mensal is null or frequencia_media_mensal = 0 then 1
        when frequencia_media_mensal >= 4 then 5
        when frequencia_media_mensal >= 2 then 4
        when frequencia_media_mensal >= 1 then 3
        when frequencia_media_mensal > 0 then 2
        else 1
    end as score_frequencia,

    case
        when total_anos_ativos > 1 and total_pedidos > 0 then
            (total_pedidos::float / total_anos_ativos) / nullif(
                (select avg(total_pedidos::float / total_anos_ativos) 
                 from pedidos_por_cliente 
                 where total_anos_ativos > 1),0)
        else 0
    end as taxa_crescimento_vs_media,

    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from pedidos_por_cliente pc
