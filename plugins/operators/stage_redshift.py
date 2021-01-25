from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON 'auto'
        """

    @apply_defaults
    def __init__(self,              
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 s3_path =  '',
                 table_name = '',
                 ignore_headers = '',
                 delimiter = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path =  s3_path
        self.table_name = table_name
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter

    def execute(self, context):       
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from {self.table_name} Redshift table")
        redshift.run("DELETE FROM {}".format(self.table_name))

        
        self.log.info("Copying data from S3 to Redshift")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table_name,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        redshift.run(formatted_sql)


