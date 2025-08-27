{{
    config(
        materialized = 'table',
        unique_key = ['sk_cliente','estacao'],
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
    case 
        when dd.month_of_year in (12,1,2) then 'Verão'
        when dd.month_of_year in (3,4,5) then 'Outono'
        when dd.month_of_year in (6,7,8) then 'Inverno'
        when dd.month_of_year in (9,10,11) then 'Primavera'
    end as estacao,
    count(distinct fp.sk_pedido) as pedidos_estacao
from dim_clientes dc
left join fact_pedidos fp on dc.sk_cliente = fp.fk_cliente
left join dim_date dd on date_trunc('day', fp.dt_pedido) = dd.date_day
group by 1,2
