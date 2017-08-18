title: Report
template: report

| hpoid   |    table      | filename | received | parsing | loaded|
|----------|:-------------:|---------:|----------|:-------:|------:|
{% for log in logs | sort(attribute='hpo_id') %}
  |{{ log.hpo_id }}  |{{ log.table_name }} |{{ log.file_name }} |{% if log.received %} &#10004; {% endif %} |{% if log.parsing %} &#10004; {% endif %} |{% if log.loading %} &#10004; {% endif %}|
{% endfor %}
