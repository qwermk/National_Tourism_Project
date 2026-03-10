-- =============================================================================
-- Test: assert_valid_occupancy_rate
-- =============================================================================
-- Verifica que la ocupación hotelera sea coherente entre calculada y reportada.
-- Si la diferencia supera 10 puntos porcentuales, algo está mal.
-- =============================================================================

select *
from {{ ref('fct_hotel_occupancy') }}
where
    abs(porcentaje_ocupacion - ocupacion_calculada) > 10
    and habitaciones_disponibles > 0
