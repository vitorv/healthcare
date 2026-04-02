{% macro get_bronze_columns_to_file() %}
    {% set query %}
        SELECT 
            TABLE_NAME, 
            ARRAY_AGG(COLUMN_NAME) AS COLUMNS
        FROM HEALTHCARE.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'BRONZE'
        GROUP BY TABLE_NAME
        ORDER BY TABLE_NAME;
    {% endset %}

    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set tables = [] %}
        {% for row in results.rows %}
            {% do tables.append({
                "table_name": row['TABLE_NAME'],
                "columns": row['COLUMNS'] | string
            }) %}
        {% endfor %}
        
        {# Write the JSON to a temp file to avoid dbt log formatting issues #}
        {% set json_str = tojson(tables) %}
        {% do log("Writing " ~ tables | length ~ " tables to temp file...", info=True) %}
        
        {# Use a Snowflake query to write a local marker, but actually just print for Python to capture #}
        {{ log("===JSON_OUTPUT_START===", info=True) }}
        {% for chunk in json_str | batch(4000) %}
            {{ log(chunk | join(''), info=True) }}
        {% endfor %}
        {{ log("===JSON_OUTPUT_END===", info=True) }}
    {% endif %}
{% endmacro %}
