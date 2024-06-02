from os import getenv, listdir, path
from google.cloud.bigquery import Client, Table


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


def prepare_and_process(request):
    prepare_bq_storage("staging", "schemas/staging")
    prepare_bq_storage("warehouse", "schemas/warehouse")
    return "success"
