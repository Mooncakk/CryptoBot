import csv
import json
import os
from datetime import date

import requests

ROOT_URL = "https://api.coingecko.com/api/v3"
HEADER = {
        "accept": "application/json",
        "x_cg_demo_api_key": "CG-fCmgjnk4Fm4pQLbin5Lr7y3G"
    }
DATE = date.today()


def get_data(coin):
    """Get coin data"""

    root_url = ROOT_URL
    header = HEADER
    response = requests.get(f'{root_url}/coins/{coin}', params=header)

    return response.json()

def get_historical_data(coin):
    """Get coin historical chart data"""

    root_url = ROOT_URL
    header = HEADER
    currency = "usd"
    time = "365"
    response = requests.get(f"{root_url}/coins/{coin}/market_chart?vs_currency={currency}&days={time}",
                             params=header)

    return response.json()

def data_to_csv(data, filename):
    """Save data to CSV file"""

    with open(f'./data/raw/{filename}', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['date', 'price'])
        writer.writerows(data)

def remove_files():

    path = './data/raw/'
    files = os.listdir(path)
    for file in files:
        os.remove(path+file)

if __name__=='__main__':

    with open('./crypto_wallet.json', 'r') as file:
        crypto_wallet = json.load(file)

    remove_files()
    for symbol in crypto_wallet:

        name = crypto_wallet.get(symbol)
        crypto_data = get_historical_data(name).get('prices')
        data_to_csv(crypto_data, f'{symbol}_data{DATE}.csv')