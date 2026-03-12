-- =============================================================================
-- initcap.sql — Implementación compatible con DuckDB de INITCAP
-- =============================================================================
-- DuckDB 1.0+ no incluye la función INITCAP nativa (disponible en PostgreSQL).
-- Este macro replica su comportamiento usando list_transform + lambda,
-- capitalizando la primera letra de cada palabra.
-- =============================================================================

{% macro initcap(col) -%}
array_to_string(
    list_transform(
        string_split(trim(coalesce({{ col }}, '')), ' '),
        w -> case when length(w) > 0
                  then upper(left(w, 1)) || lower(substr(w, 2))
                  else w
             end
    ),
    ' '
)
{%- endmacro %}
