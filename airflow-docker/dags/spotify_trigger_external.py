from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    "owner":"prasun",
    "depends_on_past": False,
    "start_date":datetime(2026, 5, 19)
}

dag = DAG(
    dag_id = "spotify_trigger_external",
    default_args = default_args,
    description = "DAG to trigger Lambda Function and Check S3 Upload",
    schedule = timedelta(days=1),
    catchup = False
)

trigger_extract_lambda = LambdaInvokeFunctionOperator(
    task_id = "trigger_extract_lambda",
    function_name = "spotify_api_data_extract",
    aws_conn_id = "aws_s3_spotify",
    region_name = "eu-north-1",
    dag = dag
)

check_s3_upload = S3KeySensor(
    task_id = "check_s3_upload",
    bucket_key = "s3://spotify-etl-project-prasun/raw_data/to_processed/*",
    wildcard_match = True,
    aws_conn_id = "aws_s3_spotify",
    timeout = 60 * 60, #wait for one hour
    poke_interval = 60,
    dag = dag
)

trigger_glue_job = GlueJobOperator(
    task_id = "trigger_glue_job",
    job_name = "spotify_transformation_job",
    script_location ="s3://aws-glue-assets-021837495457-eu-north-1/scripts/spotify_transformation_job.py", #provide complete script
    aws_conn_id = "aws_s3_spotify",
    region_name = "eu-north-1",
    iam_role_name = "spotify_glue_iam_role",
    s3_bucket = "aws-glue-assets-021837495457-eu-north-1"
)

trigger_extract_lambda >> check_s3_upload >> trigger_glue_job