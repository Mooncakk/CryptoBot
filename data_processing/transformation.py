import logging

import pandas as pd
import boto3
import awswrangler as wr


S3 = boto3.resource('s3')

if __name__=='__main__':

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    bucket_name = ('s3bucket-cryptobot')
    bucket = S3.Bucket(bucket_name)
    raw_data_path = 'data/raw/'
    raw_files = bucket.objects.filter(Prefix=raw_data_path)
    processed_files_path = 'data/processed'
    bucket.objects.filter(Prefix=processed_files_path).delete()

    for file in raw_files:
        df = wr.s3.read_csv(f's3://s3bucket-cryptobot/{file.key}',
                            names=['date', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['date'], unit='ms')
        filename = file.key.replace(raw_data_path, '')
        wr.s3.to_csv(df, f's3://s3bucket-cryptobot/{processed_files_path}/processed_{filename}', index=False)

    logging.info('Data processed')
