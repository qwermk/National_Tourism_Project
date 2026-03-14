-- =============================================================================
-- stg_banrep_tourism_balance.sql — Balanza Turística BanRep (Silver)
-- =============================================================================
-- Limpieza de datos de la balanza de pagos turística del Banco de la República.
-- =============================================================================

with source as (

    select * from {{ source('banrep', 'tourism_balance') }}

),

cleaned as (

    select
        -- Periodo
        cast(year as integer)                                                   as year,
        cast(quarter as integer)                                                as quarter,

        -- Métricas (millones de USD)
        coalesce(cast(tourism_income_usd_millions as double), 0.0)              as tourism_income_usd_millions,
        coalesce(cast(tourism_expenditure_usd_millions as double), 0.0)         as tourism_expenditure_usd_millions,
        coalesce(cast(tourism_balance_usd_millions as double), 0.0)             as tourism_balance_usd_millions,

        -- Metadata
        coalesce(trim(source), 'banrep')                                        as source

    from source
    where
        year >= 2006
        and quarter between 1 and 4

)

select * from cleaned
