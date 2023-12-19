from verdant_dgen_setup import *
# from opto_functions import *
with sqla_pg_con(pg_creds, db='dgen_db').connect() as pg_con:
    query = f"""SELECT schema_name FROM information_schema.schemata WHERE schema_name like '%diffusion_results%';;"""
    schema_list = (sqlio.read_sql_query(sqlalchemy.text(query), pg_con))

print(schema_list)

all_schemas = schema_list['schema_name'].unique().tolist()

empty_schema_list = []
for schema in all_schemas[0:None]:
    with sqla_pg_con(pg_creds, db='dgen_db').connect() as pg_con:
        query = f'''SELECT table_schema, table_name,
                        pg_relation_size('"'||table_schema||'"."'||table_name||'"'),
                        pg_size_pretty(pg_relation_size('"'||table_schema||'"."'||table_name||'"'))
                    FROM information_schema.tables
                    WHERE table_catalog = 'dgen_db' AND
                          table_schema = '{schema}' 
                    ORDER BY table_name; '''
        # print(query)
        tables = (sqlio.read_sql_query(sqlalchemy.text(query), pg_con))
        print(f'Agent outputs for {schema}:')
        print(tables.query('table_name=="agent_outputs"').head())
        if tables.query('table_name=="agent_outputs"')['pg_relation_size'].values[0] == 0:
            empty_schema_list.append(schema)

print(empty_schema_list)



for schema in empty_schema_list:
    with psyc_pg_con(pg_creds, db='dgen_db') as conn:
        cur = conn.cursor()
        query = f""" DROP SCHEMA IF EXISTS {schema} CASCADE;"""
        print(query)
        cur.execute(query)

