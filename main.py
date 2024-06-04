from os import getenv, listdir, path
from google.cloud.bigquery import Client, Table
from google.cloud.bigquery_datatransfer import (
    DataTransferServiceClient,
    TransferConfig,
    CreateTransferConfigRequest,
)


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
    parent = transfer_client.common_project_path(getenv("GCP_PROJECT_ID"))

    for file_name in listdir(folder_path):
        with open(path.join(folder_path, file_name), "r") as fp:
            query_string = fp.read()
        transfer_config = TransferConfig(
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


def prepare_and_process(request):
    prepare_bq_storage("staging", "schemas/staging")
    prepare_bq_storage("warehouse", "schemas/warehouse")
    schedule_sql_query("queries/warehouse", "every day 00:00")
    schedule_sql_query("queries/staging", "every day 01:00")
    return "success"
