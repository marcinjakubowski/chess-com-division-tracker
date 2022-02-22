{% macro match_username(when_white, when_black) %}
CASE
  WHEN games.white_username = games.username THEN {{when_white}}
  ELSE {{when_black}}
END
{% endmacro %}