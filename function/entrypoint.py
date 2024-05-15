from os import getenv
from sqlalchemy import create_engine
from sqlalchemy.sql import text

def handler(event, context):
    username, password = getenv("DWH_USER"), getenv("DWH_PASSWORD")
    host, database = getenv("DWH_HOST"), getenv("DWH_NAME")
    engine = create_engine(
        f"redshift+redshift_connector://{username}:{password}@{host}/{database}"
    )
    conn = engine.connect()
    with open("warehouse.sql", "r") as fp:
        for query in fp.read().split(";"):
            conn.execute(text(query.strip("\n\t\r ")))
    conn.close()
    engine.dispose()
    return "success"
