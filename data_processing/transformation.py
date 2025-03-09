import os
from datetime import date

import pandas as pd

DATE = date.today()


def remove_files(path):

    files = os.listdir(path)
    for file in files:
        os.remove(f'{path}/{file}')

if __name__=='__main__':

    filenames = os.listdir('./data/raw')
    raw_data_dir = './data/raw'
    processed_files_dir = './data/processed'
    remove_files(processed_files_dir)

    for filename in filenames:

        df = pd.read_csv(f'{raw_data_dir}/{filename}')
        df['date'] = pd.to_datetime(df['date'], unit='ms').dt.date
        df = df.dropna()
        df = df.drop_duplicates(subset=['date'], keep='last')
        df.to_csv(f'{processed_files_dir}/{filename}', index=False)




