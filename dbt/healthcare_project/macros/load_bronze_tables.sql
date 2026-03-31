{% macro load_bronze_tables(stage_name='HEALTHCARE_S3_STAGE', file_format_name='MY_CSV_FORMAT') %}

    {# 
        This macro executes a pure Snowflake Script to dynamically process
        all CSV files inside a Stage, infer their schemas, create tables, 
        and copy the data into them seamlessly. 
    #}

    {% set query %}
        EXECUTE IMMEDIATE $$
        DECLARE
            file_name VARCHAR;
            table_name VARCHAR;
            create_stmt VARCHAR;
            copy_stmt VARCHAR;
            
            -- Find all files currently residing in the staging layer
            c1 CURSOR FOR 
                SELECT DISTINCT METADATA$FILENAME 
                FROM @{{ stage_name }};
        BEGIN
            FOR file_record IN c1 DO
                file_name := file_record.METADATA$FILENAME;
                
                -- Extract just the base file name (without path or .csv extension) 
                -- to use as the clean table name.
                table_name := REPLACE(REGEXP_REPLACE(file_name, '^.*/(.*)\\.csv$', '\\1'), '-', '_');
                
                -- 1. Create the table dynamically by inferring the schema from the raw file
                create_stmt := 'CREATE TABLE IF NOT EXISTS IDENTIFIER(''' || table_name || ''')
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(INFER_SCHEMA(
                            LOCATION => ''@{{ stage_name }}/' || file_name || ''',
                            FILE_FORMAT => ''{{ file_format_name }}''
                        ))
                    )';
                
                EXECUTE IMMEDIATE create_stmt;
                
                -- 2. Load the actual data into the newly created schema
                copy_stmt := 'COPY INTO IDENTIFIER(''' || table_name || ''')
                    FROM ''@{{ stage_name }}/' || file_name || '''
                    FILE_FORMAT = (FORMAT_NAME = ''{{ file_format_name }}'')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = ''CONTINUE''';
                    
                EXECUTE IMMEDIATE copy_stmt;
                
            END FOR;
            
            RETURN 'Successfully ingested all raw csv files into Bronze layer.';
        END;
        $$;
    {% endset %}

    {# execute the query inside snowflake #}
    {% do run_query(query) %}
    
    {{ log("Successfully ingested Bronze Tables from Stage: " ~ stage_name, info=True) }}

{% endmacro %}
