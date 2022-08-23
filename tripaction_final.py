import collections
from tabnanny import verbose
import requests
import pandas as pd
import time
import logging
from google.cloud import bigquery
import pytz
from pytz import timezone
from datetime import date, datetime
import warnings
import os

project_id = 'turing-dev-337819'
bq_client = bigquery.Client(project=project_id)
destination_table = 'data_engineering.tripaction'

start_epoch_time = 0
current_epoch_time = int(time.time())

def get_token():
    data = {
        'grant_type': 'client_credentials',
        'client_id': '<YOUR_KEY>',
        'client_secret': '<YOUR_SECRET>',
    }

    response = requests.post(
        'https://api.tripactions.com/ta-auth/oauth/token', data=data)

    token = response.json()
    token = 'Bearer '+token['access_token']
    return token

token = get_token()

bq_date_format = '%Y-%m-%d'

schema_tripaction = [
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("costCenters", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("travelMonth", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("bookingStatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("vendor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("bookingType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("startDate", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("usdGrandTotal", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("saving", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("optimalPrice", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("paymentSchedule", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("paymentMethodUsed", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tripDescription", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("inventory", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("purpose", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tripName", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
]

headers = {
    'Authorization': token,
}
params = {
    'createdFrom': start_epoch_time,
    'createdTo': current_epoch_time,
    'page': '0',
    'size': '100',
}

response = requests.get(
    'https://api.tripactions.com/v1/bookings', params=params, headers=headers)
resp = response.json()
count = (resp['page']['totalPages'])
page = 0

df = pd.DataFrame()

for i in range(0, count):
    headers = {
        'Authorization': token,
    }
    params = {
        'createdFrom': start_epoch_time,
        'createdTo': current_epoch_time,
        'page': page,
        'size': '100',
    }

    response = requests.get(
        'https://api.tripactions.com/v1/bookings', params=params, headers=headers)
    try:
        resp = response.json()
    except ValueError:
        print('Invalid values in json')
    temp_df = pd.json_normalize(resp['data'])
    temp_df['travelMonth'] = pd.to_datetime(
        temp_df['startDate']).dt.strftime('%b-%y')
    df = pd.concat([df, temp_df], ignore_index=True)
    page = page+1
# print(df)
    
df_final = df[['booker.name', 'booker.email',  'costCenters', 'travelMonth', 'tripName', 'usdGrandTotal', 'saving', 'optimalPrice', 'paymentSchedule', 'paymentMethodUsed',
'purpose', 'tripDescription', 'inventory', 'bookingStatus', 'startDate', 'vendor', 'bookingType', 'uuid']]
df_final.rename(columns={'booker.name':'name','booker.email':'email'},inplace=True)
df_final[['startDate']] = df_final[['startDate']].apply(
    pd.to_datetime, format=bq_date_format, utc=True)
df_final['costCenters']=df_final['costCenters'].astype(str)
df_final['costCenters'] = df_final['costCenters'].str.extract(r"'([^']*)'")
# print(df_final.dtypes)
job_config = bigquery.LoadJobConfig(
    write_disposition='WRITE_TRUNCATE', autodetect=False, allow_quoted_newlines=True, schema=schema_tripaction)

load_job = bq_client.load_table_from_dataframe(
    df_final, destination_table, job_config=job_config)
