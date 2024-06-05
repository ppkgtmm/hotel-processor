from os import getenv, listdir, path
from google.cloud.bigquery import Client, Table
from google.cloud.bigquery_datatransfer import (
    DataTransferServiceClient,
    TransferConfig,
    CreateTransferConfigRequest,
)
from google.cloud.dataproc import JobControllerClient, Job, PySparkJob, JobPlacement


def prepare_bq_storage(dataset_name: str, schema_dir: str):
    bq_client = Client()
    dataset_ref = getenv("GCP_PROJECT_ID") + "." + dataset_name
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    for file_name in listdir(schema_dir):
        file_path = path.join(schema_dir, file_name)
        table_name = file_name.split(".")[0]
        table = Table(
            dataset_ref + "." + table_name, schema=bq_client.schema_from_json(file_path)
        )
        bq_client.delete_table(table, not_found_ok=True)
        bq_client.create_table(table)


def schedule_sql_query(folder_path: str, schedule_interval: str):
    service_account = getenv("SERVICE_ACCOUNT")
    transfer_client = DataTransferServiceClient()
    parent = f"projects/{getenv("GCP_PROJECT_ID")}/locations/US"

    for file_name in listdir(folder_path):
        with open(path.join(folder_path, file_name), "r") as fp:
            query_string = fp.read()
        transfer_config = TransferConfig(
            name=f"projects/{getenv("GCP_PROJECT_ID")}/locations/US/transferConfigs",
            display_name=file_name + "_query",
            data_source_id="scheduled_query",
            params={"query": query_string},
            schedule=schedule_interval,
        )
        transfer_config = transfer_client.create_transfer_config(
            CreateTransferConfigRequest(
                parent=parent,
                transfer_config=transfer_config,
                service_account_name=service_account,
            )
        )


def populate_time_dimension():
    hours, mins = list(range(0, 24)), [0, 30]
    data = []
    for hour in hours:
        for minute in mins:
            data.append(
                dict(
                    id=int(f"{hour}{minute:02d}00"), hour=hour, minute=minute, second=0
                )
            )
    Client().load_table_from_json(data, "warehouse.dim_time").result()


def submit_streaming_job():
    job_client = JobControllerClient(
        client_options={
            "api_endpoint": f"{getenv("GCP_REGION")}-dataproc.googleapis.com:443"
        }
    )
    pyspark_job = PySparkJob()
    pyspark_job.main_python_file_uri = f"gs://{getenv("BUCKET_NAME")}/pyspark/main.py"
    pyspark_job.python_file_uris = [
        f"gs://{getenv("BUCKET_NAME")}/pyspark/dependencies.zip"
    ]
    pyspark_job.jar_file_uris = [
        "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.39.0.jar",
        "gs://spark-lib/pubsublite/pubsublite-spark-sql-streaming-LATEST-with-dependencies.jar",
        "gs://hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
    ]
    pyspark_job.args = [
        getenv("GCP_PROJECT_ID"),
        getenv("GCP_ZONE"),
        getenv("BUCKET_NAME"),
    ]
    pyspark_job.properties = {
        "dataproc:pip.packages": "db-dtypes==1.2.0,google-cloud-bigquery==3.23.1,pandas==2.2.2",
        "spark:spark.submit.deployMode": "cluster",
    }
    job = Job()
    job.placement = JobPlacement(cluster_name=getenv("CLUSTER_NAME"))
    job.pyspark_job = pyspark_job

    job_client.submit_job(
        project_id=getenv("GCP_PROJECT_ID"), region=getenv("GCP_REGION"), job=job
    )


def prepare_and_process(request):
    prepare_bq_storage("staging", "schemas/staging")
    prepare_bq_storage("warehouse", "schemas/warehouse")
    populate_time_dimension()
    schedule_sql_query("queries/warehouse", "every day 00:00")
    schedule_sql_query("queries/staging", "every day 01:00")
    submit_streaming_job()
    return "success"
