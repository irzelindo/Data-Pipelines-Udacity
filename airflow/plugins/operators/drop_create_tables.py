from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DropCreateTablesOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 aws_credentials = {},
                 sql_queries = None,
                 *args, **kwargs):

        super(DropCreateTablesOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.sql_queries = sql_queries
        
        
    def execute(self, context):
        self.log.info(f'Dropping and Creating tables')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for query in self.sql_queries:
            redshift.run(query.format(self.aws_credentials.get('key'), self.aws_credentials.get('secret')))

            
            
         