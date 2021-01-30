from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = '''INSERT INTO ({}); '''
    truncate_sql = ''' TRUNCATE TABLE {}'''


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',    
                 table_name = '',
                 sql_code = "",
                 insert_type = '', # INSERT or TRUNCATE
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name.upper()
        self.sql_code = sql_code
        self.insert_type = insert_type

    def execute(self, context):
        if not (self.insert_type == 'INSERT' or self.insert_type == 'TRUNCATE'):
            raise ValueError(f"insert type {self.insert_type} is not insert or truncate")
        
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.insert_type == 'TRUNCATE':
            self.log.info(f"Truncating {self.table_name} Redshift dimension table")
            redshift.run(self.truncate_sql.format(self.table_name))

           
        self.log.info(f"Inserting into {self.table_name} Redshift dimension table")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table_name,
            self.sql_code
        )
        redshift.run(formatted_sql)
        