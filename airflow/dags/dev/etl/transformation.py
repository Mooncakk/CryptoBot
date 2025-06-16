import logging
import json

import pandas as pd
import boto3
import awswrangler as wr


S3 = boto3.resource('s3')


def open_params(filename: str) -> str:
    """Open a json file and gets different parameters"""

    with open(filename, 'r') as file:
        params = json.load(file)

    aws_params = params.get('aws_params')
    bucket_name = aws_params.get('s3bucket_name')

    return bucket_name


def main() -> None:

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    raw_data_path = 'data/raw/'
    bucket_name = open_params('./utils/utils.json')
    bucket = S3.Bucket(bucket_name)
    bucket_url = f's3://{bucket_name}'
    raw_files = bucket.objects.filter(Prefix=raw_data_path)
    processed_files_path = 'data/processed'
    bucket.objects.filter(Prefix=processed_files_path).delete()

    for file in raw_files:
        df = wr.s3.read_csv(f'{bucket_url}/{file.key}',
                            names=['date', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['date'], unit='ms')
        filename = file.key.replace(raw_data_path, '')
        wr.s3.to_csv(df, f'{bucket_url}/{processed_files_path}/processed_{filename}', index=False)

    logging.info('Data processed')

if __name__=='__main__':

    main()