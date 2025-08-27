{{
    config(
        materialized = 'table',
        unique_key = ['sk_cliente','dia_semana'],
        tags = ['mart','metrics']
    )
}}

with 
fact_pedidos as (
    select *, cast(dt_pedido as date) as data_pedido
    from {{ ref('int_fact_pedidos') }}
),
dim_clientes as (
    select * from {{ ref('int_dim_clientes') }}
),
dim_date as (
    select * from {{ ref('int_dim_date') }}
)

select 
    dc.sk_cliente,
    dd.day_of_week_name as dia_semana,
    case when count(distinct fp.sk_pedido) is null then 0 else count(distinct fp.sk_pedido) end as pedidos_semana
from dim_clientes dc
left join fact_pedidos fp on dc.sk_cliente = fp.fk_cliente
left join dim_date dd on date_trunc('day', fp.dt_pedido) = dd.date_day
group by 1,2
