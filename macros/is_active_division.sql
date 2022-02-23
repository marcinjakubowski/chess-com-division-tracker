{% macro is_active_division(division_column) %}
EXISTS (SELECT 1 
          FROM {{ source('chesscom', 'division') }} is_active_division 
         WHERE is_active_division.is_active 
           AND is_active_division.id = {{division_column}})
{% endmacro %}