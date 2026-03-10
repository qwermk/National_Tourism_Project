-- =============================================================================
-- Test: assert_positive_values — Verifica que una columna tenga valores >= 0
-- =============================================================================
-- Uso en YAML:
--   tests:
--     - assert_positive_values:
--         column_name: total_visitantes
-- =============================================================================

{% test assert_positive_values(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} < 0

{% endtest %}
