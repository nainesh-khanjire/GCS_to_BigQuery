# import all necessary packages and modules
import json
from datetime import datetime, timedelta,timezone
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from airflow.contrib.operators import gcs_to_bq
#from airflow.providers.google.cloud.transfers.gcs_to_bq import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator # for insert
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

#from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryColumnCheckOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteDatasetOperator

from airflow.models import TaskInstance
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup


DATASET = "simple_bigquery_dag" # define dataset name
TABLE = "forestfires" # define target table name
yesterday = datetime.now() - timedelta(days=1)
location = 'us-east1' # cloud composer location

# webhook url for slack app that will send messages to assigned channels or profiles
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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" 


default_args = {
    'owner': 'Cervello_GCP', # DAG owner name
    'start_date': yesterday,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0), # time span between failure and next retry
}

# <<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>

# <<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>><<<<<<<<>>>>>>>>>>>

# slack notification function gets called on task failure
def send_slack_notification(context):
    if SLACK_WEBHOOK_URL:
        failed_task = context.get("task_instance").task_id
        msg = f":red_circle: Hello {default_args['owner']} ! Your Airflow Task with Task ID *{failed_task}* has been failed in DAG *{context.get('dag_run').dag_id}*"
        
        SlackWebhookHook(
            webhook_token=SLACK_WEBHOOK_URL,
            message=msg,
        ).execute()


# slack notification function gets called on task success
def dag_success_notification(context):
        if SLACK_WEBHOOK_URL:
            succeeded_task = context.get("task_instance").task_id
            msg = f":large_green_circle: Hello {default_args['owner']} ! Your Airflow Task with Task ID *{succeeded_task}* has been executed successfully in DAG *{context.get('dag_run').dag_id}*"
        
            SlackWebhookHook(webhook_token=SLACK_WEBHOOK_URL, message=msg).execute()



with DAG(
    'Forest_fires_data_quality_check', # this is dag ID
    default_args=default_args,
    schedule_interval= '0 10 * * *', # daily execution at 10am, similar to cron expression
    description='DAG for data quality check with BigQuery', # this is dag detail
    template_searchpath="/home/airflow/gcs/data/", # path for json validation file
    catchup=False, # to avoid backfilling of task executions
) as dag:


    begin = DummyOperator(task_id="begin",dag=dag) # dummy operator
    
    # BigQuery dataset creation
    # Create empty  dataset to store the sample data tables


    create_dataset = BigQueryCreateEmptyDatasetOperator( # creating dataset in BQ using dataset name
        task_id="create_dataset",
        on_failure_callback=send_slack_notification,
        on_success_callback = dag_success_notification, 
        dataset_id=DATASET
    )

    
    # BigQuery table creation
    # Create empty  table to store sample forest fire data
    create_table = BigQueryCreateEmptyTableOperator( 
        task_id="create_table",
        on_failure_callback=send_slack_notification,
        on_success_callback = dag_success_notification,
        dataset_id=DATASET,
        table_id=TABLE, # defining the schema for the table 
        schema_fields=[ 
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "month", "type": "STRING", "mode": "NULLABLE"},
            {"name": "day", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ffmc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dmc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "isi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "temp", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rh", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "wind", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rain", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "area", "type": "FLOAT", "mode": "NULLABLE"},
        ]
    )

    # BigQuery table check
    # Ensure that the table was created in BigQuery before inserting data

    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_for_table", # task name
        project_id="{{ var.value.gcp_project_id }}", # using  airflow variables for gcp project id 
        on_failure_callback=send_slack_notification,
        on_success_callback = dag_success_notification,
        dataset_id=DATASET,
        table_id=TABLE
    )   

    # Insert data
    # Insert data into the BigQuery table using an existing SQL query (stored in a file under data/ folder)
 
    load_data = GoogleCloudStorageToBigQueryOperator( 
    task_id='insert_data',
    bucket='us-central1-composer-1-4c495ead-bucket', # cloud composer storage buckete name
    source_objects=['data/forestfires.csv'], # csv file to create the table
    destination_project_dataset_table= 'cervellogcp.simple_bigquery_dag.forestfires', # projectid.datasetid.tablename
    on_failure_callback=send_slack_notification,
    on_success_callback = dag_success_notification,
    source_format='CSV', 
    write_disposition='WRITE_TRUNCATE', # truncates table before every insert
    autodetect=False, # to avoid overwrting of headers while loading data from csv where headers are not present
    schema_fields=[
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "month", "type": "STRING", "mode": "NULLABLE"},
    {"name": "day", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ffmc", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "dmc", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "dc", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "isi", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "temp", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "rh", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "wind", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "rain", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "area", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    dag=dag
    )


    # Row-level data quality check
    # Run data quality checks on a few rows, ensuring that the data in BigQuery
    # matches the ground truth in the correspoding JSON file

    with open("/home/airflow/gcs/data/forestfire_validation.json",'r') as ffv:
        with TaskGroup(group_id="row_quality_checks") as quality_check_group:
            ffv_json = json.load(ffv)
            for id, values in ffv_json.items():
                values["id"] = id
                values["dataset"] = DATASET
                values["table"] = TABLE
                BigQueryCheckOperator(
                    task_id=f"check_row_data_{id}",
                    on_failure_callback=send_slack_notification,
                    on_success_callback = dag_success_notification,
                    sql="row_quality_bigquery_forestfire_check.sql",
                    use_legacy_sql=False,
                    dag= dag,
                    params=values,
                )


    column_check = BigQueryColumnCheckOperator(
        task_id="column_value_check",
        table=f"{DATASET}.{TABLE}",
        column_mapping={"id": {"null_check": {"equal_to": 0}}},
        on_failure_callback=send_slack_notification,
        on_success_callback = dag_success_notification,
        dag=dag
    )

    
    # Table-level data quality check
    # Run a row count check to ensure all data was uploaded to BigQuery properly
    check_bq_row_count = BigQueryValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        on_failure_callback=send_slack_notification,
        on_success_callback = dag_success_notification,
        pass_value=9,
        dag=dag,
        use_legacy_sql=False,
    )

    """
    #### Delete test dataset and table
    Clean up the dataset and table created for the example.
    """
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        on_failure_callback=send_slack_notification,
        on_success_callback = dag_success_notification, 
        dataset_id=DATASET, 
        dag=dag,
        trigger_rule='all_success',
        delete_contents=True
    )

    end = DummyOperator(task_id="end",dag=dag,trigger_rule='all_success')
    # trigger rulem all success means it will execute when all previous tasks are succeed

# task dependency can be assigned using chain or conventional
    chain(
        begin,
        create_dataset,
        create_table,
        check_table_exists,
        load_data,
        [quality_check_group,check_bq_row_count,column_check],
        delete_dataset,
        end,
    )

#begin >> create_dataset >> create_table >> check_table_exists >> load_data >> [quality_check_group, check_bq_row_count] >> column_check >> delete_dataset>> end