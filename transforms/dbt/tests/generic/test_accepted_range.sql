{% test accepted_range(model, column_name, min_value, max_value, inclusive=true) %}
{#-
    A dbt test fails when it RETURNS rows.
    So we select the out-of-range values: zero rows means the test passes.
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