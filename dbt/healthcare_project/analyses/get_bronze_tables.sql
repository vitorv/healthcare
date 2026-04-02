{% macro get_bronze_tables() %}
    {% set query %}
        SELECT table_name 
        FROM HEALTHCARE.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'BRONZE'
        AND TABLE_TYPE = 'BASE TABLE';
    {% endset %}

    {% set results = run_query(query) %}
    
    {% if execute %}
        {% for row in results.rows %}
            {{ log("TABLE: " ~ row['TABLE_NAME'], info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %}
