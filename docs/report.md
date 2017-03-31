{% assign last_updated = site.data.report_info.last_run_seconds | date: '%B %d, %Y %I:%M %p' %}
<p><i>Last updated: {{ last_updated }}</i> (GMT)</p>
<table class="log-table">
<thead>
<tr>
  <th>hpo_id</th>
  <th>table</th>
  <th>filename</th>
  <th title="A file with the given name was received via file transfer">received</th>
  <th title="The file was parsed successfully as a CSV file">parsed</th>
  <th title="All rows in the file had the minimum required fields and fields were of the correct type">loaded</th>
</tr>
</thead>
<tbody>
{% assign log_items = site.data.log | sort: 'log_id' %}
{% for log in log_items %}
<tr {% if log.message != null %} class="issue" data-message="{{ log.message | xml_escape }}" {% endif %}>
  <td>{{ log.hpo_id }}</td>
  <td>{{ log.table_name }}</td>
  <td>{{ log.file_name }}</td>
  <td>{% if log.received %} &#10004; {% endif %}</td>
  <td>{% if log.parsing %} &#10004; {% endif %}</td>
  <td>{% if log.loading %} &#10004; {% endif %}</td>
</tr>
{% endfor %}
</tbody>
</table>
