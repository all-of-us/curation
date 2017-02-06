<table>
<thead>
<tr>
  <th>hpo_id</th>
  <th>table</th>
  <th>step</th>
  <th></th>
</tr>
</thead>
<tbody>
{% assign log_items = site.data.log | sort: 'log_id' %}
{% for log in log_items %}
<tr>
  <td>{{ log.hpo_id }}</td>
  <td>{{ log.table_name }}</td>
  <td>{{ log.phase }}</td>
  <td>{% if log.success %} &#10004; {% endif %}</td>
</tr>
{% endfor %}
</tbody>
</table>
