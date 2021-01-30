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
        JSON '{}'
        """
    template_fields = ("s3_path",)
    
    @apply_defaults
    def __init__(self,              
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 s3_path = '',
                 table_name = '',
                 json = 'auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path =  s3_path
        self.table_name = table_name
        self.json = json

    def execute(self, context):    
        self.log.info(f"Creating {self.table_name} Redshift stage table")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Removing previous records")
        redshift.run("TRUNCATE TABLE {}".format(self.table_name))
        
        if self.table_name == 'staging_events':
            path = self.s3_path.format(**context)
        else:
            path = self.s3_path

        self.log.info("Copying data from S3 to Redshift")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table_name,
            path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        redshift.run(formatted_sql)


