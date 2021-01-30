from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadDimensionOperator,
                               LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay' :  timedelta(minutes = 5), 
    'catchup' : False,
    'email_on_retry' : False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_path =  '''s3://udacity-dend/log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json''',
    json = "s3://udacity-dend/log_json_path.json",
    table_name = 'staging_events' 
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_path =  "s3://udacity-dend/song_data",
    table_name = 'staging_songs'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',    
    table_name = 'songplays',
    sql_code = SqlQueries.songplay_table_insert
)
    
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',    
    table_name = 'users',
    sql_code = SqlQueries.user_table_insert,
    insert_type = 'INSERT' # INSERT or TRUNCATE
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',    
    table_name = 'songs',
    sql_code = SqlQueries.song_table_insert,
    insert_type = 'INSERT' # INSERT or TRUNCATE
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',    
    table_name = 'artists',
    sql_code = SqlQueries.artist_table_insert,
    insert_type = 'INSERT' # INSERT or TRUNCATE

)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',    
    table_name = 'time',
    sql_code = SqlQueries.time_table_insert,
    insert_type = 'INSERT' # INSERT or TRUNCATE
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    table_names = ['time','user','artists','songplays','songs'],
    redshift_conn_id = 'redshift',
    dag=dag,
    sql_code = [
        'Select count(*) FROM artists where artistid is null',
        'Select count(*) FROM songplays where playid is null or start_time is null',
        'Select count(*) FROM songs where songid is  null',
        'Select count(*) FROM time where start_time is  null',
        'Select count(*) FROM users where userid is null']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Dependancies

start_operator >> create_tables >> [stage_events_to_redshift,stage_songs_to_redshift] \
>> load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, \
                           load_artist_dimension_table,load_time_dimension_table] \
>> run_quality_checks >> end_operator