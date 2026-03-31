{% macro load_bronze_tables(stage_name='HEALTHCARE_S3_STAGE', file_format_name='MY_CSV_FORMAT') %}

    {# 
        This macro executes a pure Snowflake Script to dynamically process
        all CSV files inside a Stage, infer their schemas, create tables, 
        and copy the data into them seamlessly. 
    #}

    {# Create a temporary file format WITHOUT header parsing to safely read filenames #}
    {% do run_query("CREATE TEMPORARY FILE FORMAT IF NOT EXISTS temp_dummy_format TYPE = CSV PARSE_HEADER = FALSE") %}

    {% set query %}
        EXECUTE IMMEDIATE $$
        DECLARE
            file_name VARCHAR;
            file_basename VARCHAR;
            table_name VARCHAR;
            create_stmt VARCHAR;
            copy_stmt VARCHAR;
            tables_created INTEGER DEFAULT 0;
            tables_list VARCHAR DEFAULT '''';
            
            -- Find all files currently residing in the staging layer
            c1 CURSOR FOR 
                SELECT DISTINCT METADATA$FILENAME 
                FROM @{{ stage_name }} (FILE_FORMAT => 'temp_dummy_format')
                WHERE METADATA$FILENAME ILIKE '%.csv';
        BEGIN
            FOR file_record IN c1 DO
                file_name := file_record.METADATA$FILENAME;
                
                -- Extract just the strictly actual filename (without directory paths)
                file_basename := REGEXP_REPLACE(file_name, '^.*/(.*)$', '\\1');
                
                -- Extract just the base file name (without path or .csv extension) 
                -- to use as the clean table name.
                table_name := REPLACE(REGEXP_REPLACE(file_basename, '\\.csv$', ''), '-', '_');
                
                -- 1. Create the table dynamically by inferring the schema from the raw file
                create_stmt := 'CREATE TABLE IF NOT EXISTS IDENTIFIER(''' || table_name || ''')
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(INFER_SCHEMA(
                            LOCATION => ''@{{ stage_name }}'',
                            FILES => (''' || file_basename || '''),
                            FILE_FORMAT => ''{{ file_format_name }}''
                        ))
                    )';
                
                BEGIN
                    EXECUTE IMMEDIATE create_stmt;
                    
                    -- 2. Load the actual data into the newly created schema
                    copy_stmt := 'COPY INTO IDENTIFIER(''' || table_name || ''')
                        FROM ''@{{ stage_name }}''
                        FILES = (''' || file_basename || ''')
                        FILE_FORMAT = (FORMAT_NAME = ''{{ file_format_name }}'')
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                        ON_ERROR = ''CONTINUE''';
                        
                    EXECUTE IMMEDIATE copy_stmt;
                    
                    tables_created := tables_created + 1;
                    
                    -- Track the names of the tables we created
                    IF (tables_list = '') THEN
                        tables_list := table_name;
                    ELSE
                        tables_list := tables_list || ', ' || table_name;
                    END IF;
                    
                EXCEPTION
                    -- Stop and return the exact error message so we aren't blind!
                    WHEN OTHER THEN
                        RETURN 'ERROR on file [' || file_name || ']: ' || SQLERRM;
                END;

                
            END FOR;
            
            RETURN 'Successfully ingested ' || tables_created || ' raw csv files into Bronze layer. \nTables loaded: ' || tables_list;
        END;
        $$;
    {% endset %}

    {# execute the query inside snowflake and fetch the result string #}
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set result_message = results.columns[0].values()[0] %}
        {{ log(result_message, info=True) }}
    {% endif %}

{% endmacro %}
