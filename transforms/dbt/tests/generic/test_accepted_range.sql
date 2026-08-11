{% test accepted_range(model, column_name, min_value, max_value, inclusive=true) %}
{#-
    Un test dbt echoue s'il RENVOIE des lignes.
    On selectionne donc les valeurs hors bornes : zero ligne = succes.
-#}
select
    {{ column_name }} as offending_value,
    count(*)          as occurrences
from {{ model }}
where {{ column_name }} is not null
  and (
    {% if inclusive %}
        {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
    {% else %}
        {{ column_name }} <= {{ min_value }} or {{ column_name }} >= {{ max_value }}
    {% endif %}
  )
group by 1
{% endtest %}