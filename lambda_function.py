import os
import pandas as pd
import awswrangler as wr
import urllib.parse

s3_cleaned = os.environ['s3_cleaned']
catalog_db_name = os.environ['catalog_db_name']
catalog_table_name = os.environ['catalog_table_name']
write_data_operation = os.environ['write_data_operation']


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print(f"bucket: {bucket}, key: {key}")

    try:
        df = wr.s3.read_json('s3://{}/{}'.format(bucket, key))

        print(f"reading {bucket}{key} done!")

        df_processed = pd.json_normalize(df['items'])

        print(f"processing {bucket}{key} done!")

        wr_response = wr.s3.to_parquet(
            df=df_processed,
            path=s3_cleaned,
            dataset=True,
            database=catalog_db_name,
            table=catalog_table_name,
            mode=write_data_operation
        )

        print(f"writing to {s3_cleaned} done!")

        return wr_response
    
    except Exception as e:
        print(e)
        raise e
