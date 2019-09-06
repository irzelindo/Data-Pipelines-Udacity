from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = '',
                 table_name = '',
                 s3_bucket = '',
                 s3_key = '',
                 path = '',
                 delimiter = '',
                 headers = 1,
                 quote_char = '',
                 file_type = '',
                 aws_credentials = {},
                 region = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.path = path
        self.delimiter = delimiter
        self.headers = headers
        self.quote_char = quote_char
        self.file_type = file_type
        self.aws_credentials = aws_credentials
        self.region = region

    def execute(self, context):
        self.log.info(f'Emptying stage table {self.table_name}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # redshift.run(f'DELETE FROM {self.table_name}')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        # s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        print(s3_path)
        # General copy statement for copying data from s3 to Redshift
        copy_statement = """
                    copy {} 
                    from '{}'
                    access_key_id '{}'
                    secret_access_key '{}'
                    region as '{}'
                """.format(self.table_name, s3_path, self.aws_credentials.get('key'), self.aws_credentials.get('secret'), self.region)
        # Finding file type to personilize the copy st.
        # Possible types csv and json
        if self.file_type == 'csv':
            file_statement = """
                        delimiter '{}'
                        ignoreheader {}
                        csv quote as '{}';
                    """.format(self.delimiter, self.headers, self.quote_char)
        if self.file_type == 'json':
            # By default copying with json auto
            file_statement = "json 'auto';"
            # In case the table name is staging_events 
            # Copy data using json path instead
            if self.table_name == 'staging_events':
                file_statement = f"json '{self.path}'"
        # Formating the final copy statement to copy data to Redshift.
        full_copy_statement = '{} {}'.format(copy_statement, file_statement)
        self.log.info('Starting to copy data from S3')
        redshift.run(full_copy_statement)
        self.log.info('Staging done!')





