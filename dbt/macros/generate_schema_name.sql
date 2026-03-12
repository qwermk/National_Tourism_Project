-- =============================================================================
-- Macro: generate_schema_name — Esquemas sin prefijo del target
-- =============================================================================
-- Por defecto dbt antepone el target schema al custom schema, generando
-- nombres como "main_gold". Este macro evita ese prefijo y usa el nombre
-- custom tal cual (ej.: "gold", "staging", "seeds").
--
-- Resultado:
--   +schema: gold     → gold.fct_tourism_arrivals     (no main_gold)
--   +schema: staging  → staging.stg_tourism_arrivals
--   sin schema        → main (target schema por defecto)
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
