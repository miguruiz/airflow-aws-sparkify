from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactsDimensionsOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = '''insert into {} ({}); '''


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',    
                 table_name = '',
                 sql_code = "",
                 empty_table = False,
                 *args, **kwargs):

        super(LoadFactsDimensionsOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_code = sql_code
        self.empty_table = empty_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if(self.empty_table):
            self.log.info(f"Clearing data from {self.table_name} Redshift table")
            redshift.run("DELETE FROM {}".format(self.table_name))

        formatted_sql = LoadFactsDimensionsOperator.insert_sql.format(
            self.table_name,
            self.sql_code
        )
        redshift.run(formatted_sql)
        