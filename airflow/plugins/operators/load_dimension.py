from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 table_name='',
                 sql_statement='',
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        self.log.info(f'Loading dimension {self.table_name}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading dimension {self.table_name}')
        if self.append_data == True:
            sql_statement = f'INSERT INTO {self.table_name} {self.sql_statement}'
            redshift.run(sql_statement)
        else:
            sql_statement = f'DELETE FROM {self.table_name}'
            redshift.run(sql_statement)
            sql_statement = f'INSERT INTO {self.table_name} {self.sql_statement}'
            redshift.run(sql_statement)
        self.log.info(f'Fact table {self.table_name} load finished')
