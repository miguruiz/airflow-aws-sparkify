from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    q = 'SELECT COUNT(*) FROM {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',    
                 sql_code = [],
                 table_names = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_code = sql_code
        self.table_names = table_names
      

    def execute(self, context):
        self.log.info(f"Validating if tables {self.table_names} are empty:")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for t in self.table_names:
            q = DataQualityOperator.q.format(t)
            result = redshift.get_records(q)
            if result is None or result[0][0] < 1:
                self.log.error("No records found")
                raise ValueError('No records found')
        
        self.log.info(f"All tables have records.")
        
        self.log.info(f"Validating null values:")
        for q in self.sql_code:
            result = redshift.get_records(q)
            if result[0][0] != 0:
                self.log.error("Validation failed")
                error_msg = f"Query {q} Failed"
                raise ValueError(error_msg)

        self.log.info("Data quality checks have been successful.")