from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = '''insert into {} ({}); '''


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',    
                 table_name = '',
                 sql_code = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_code = sql_code

    def execute(self, context):
        self.log.info(f"Creating {self.table_name} Redshift fact table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table_name,
            self.sql_code
        )
        redshift.run(formatted_sql)
        