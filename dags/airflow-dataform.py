import datetime
from airflow import models
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from google.cloud.dataform_v1beta1 import WorkflowInvocation
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

#Import pendulum for time zone awareness
import pendulum 

# Define your project and bucket details
PROJECT_ID = ""
GCS_BUCKET_NAME = ""
S3_BUCKET_NAME = ""
BQ_DATASET_NAME = ""
BQ_TABLE_NAME = ""

S3_BUCKET_NAME_DEST = ""

# Dataform-specific details
REPOSITORY_ID = ""
REGION = ""
GIT_COMMITISH = "main"  # or your specific branch/commit

default_args = {

    'start_date': pendulum.datetime(2024, 10, 9, tz="Europe/London"),  # Aware start date
    'author': 'Carlos Medina',
    'retries': 1,
    # Retry on failure after 5 minutes
    'retry_delay': datetime.timedelta(minutes=5),
}

with models.DAG(
    'holidays_s3_to_gcs_to_bq_with_dataform_dag',
    default_args=default_args,
    #schedule_interval='30 9 * * *',  # Cron expression for 9:30 AM every day
    schedule_interval='@once',  # Run once immediately
    catchup=False,
) as dag:
    
    # Use this task only when you want to know the encoded URI of the AWS connection for your secret manager
    print_conn_uri = PythonOperator(
        task_id='print_conn_uri',
        python_callable=lambda: print(BaseHook.get_connection('aws_default').get_uri()),
    )

    # Transfer holidays.csv from S3 to GCS
    s3_to_gcs = S3ToGCSOperator(
        task_id='s3_to_gcs',
        bucket=S3_BUCKET_NAME,
        dest_gcs=f'gs://{GCS_BUCKET_NAME}/',  
        gcp_conn_id='google_cloud_default',
        aws_conn_id='aws_default',
        deferrable=True,
        poll_interval= 4,
        replace=True
    )

    # Load holidays.csv from GCS to BigQuery
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=GCS_BUCKET_NAME,
        source_objects=['holidays.csv'],
        destination_project_dataset_table=f'{PROJECT_ID}:{BQ_DATASET_NAME}.{BQ_TABLE_NAME}',
        schema_fields=[
            {'name': 'Date', 'type': 'DATE'},
            {'name': 'Holiday', 'type': 'STRING'},
        ],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,  # Skip the header row
        allow_quoted_newlines=True,  # In case there are any quoted newlines in the Holiday names
    )

    # Create Dataform compilation result
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": GIT_COMMITISH,
        },
    )

    # Create and run Dataform workflow invocation
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        asynchronous=True,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )

    # Wait for Dataform workflow to complete
    is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=("{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"),
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
    )

    # move the table to the cloud storage bucket
    bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs",
        source_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        destination_cloud_storage_uris=[f"gs://{GCS_BUCKET_NAME}/exported_holidays_{{{{ execution_date.strftime('%Y%m%d_%H') }}}}.csv"],
        export_format="CSV",
        field_delimiter=",",
        print_header=True,
        force_rerun=True,
    )
    
    # Move the CSV file from GCS to Amazon S3
    gcs_to_s3 = GCSToS3Operator(
        task_id="gcs_to_s3",
        gcs_bucket=GCS_BUCKET_NAME,
        prefix="exported_holidays",
        dest_s3_key=f"s3://{S3_BUCKET_NAME_DEST}/google-dataform/",  # Ensure this ends with a forward slash
        gcp_conn_id="google_cloud_default",
        dest_aws_conn_id="aws_default",
        replace=True,
    )

    
# Set up the task dependencies
print_conn_uri >> s3_to_gcs >> gcs_to_bq >> create_compilation_result >> create_workflow_invocation >> is_workflow_invocation_done >> bigquery_to_gcs >> gcs_to_s3