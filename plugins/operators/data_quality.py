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
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for t in self.table_names:
            q = DataQualityOperator.q.format(t)
            logging.info(q)
            result = redshift.get_records(q)
            if result is None or result[0][0] < 1:
                logging.error("No records found")
                raise ValueError('No records found')
            
        for q in self.sql_code:
            logging.info(q)
            result = redshift.get_records(q)
            logging.info(result)
            if result[0][0] != 0:
                logging.error("Validation faild")
                error_msg = f"Query {q} Failed"
                raise ValueError(error_msg)

        logging.info("Data quality checks have been successful.")