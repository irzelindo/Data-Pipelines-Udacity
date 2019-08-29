def drop_tables(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")
    for query in SqlQueries.drop_table_queries:
        redshift_hook.run(query.format(AWS_KEY, AWS_SECRET))

       
def create_tables(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")
    for query in SqlQueries.create_table_queries:
        redshift_hook.run(query.format(AWS_KEY, AWS_SECRET))