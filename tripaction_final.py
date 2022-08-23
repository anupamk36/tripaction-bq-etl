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

# Variables to create epoch time
start_epoch_time = 0
current_epoch_time = int(time.time())


# Function to generate token since the token stays alive for only 12 hours
def get_token():
    data = {
        'grant_type': 'client_credentials',
        'client_id': '9c62f45f-4bad-4282-8ec1-ef0f223d2c28',
        'client_secret': 'b8ea2d23b77a4ad2abec71a1e435bf20',
    }

    response = requests.post(
        'https://api.tripactions.com/ta-auth/oauth/token', data=data)

    token = response.json()
    token = 'Bearer '+token['access_token']
    return token


# Storing the token in a variable
token = get_token()
print(token)
# Date format variable to convert fields fetched through API into BigQuery compatible Datatype.
bq_date_format = '%Y-%m-%d'

# Schema for passengers table.
schema_passengers = [
    bigquery.SchemaField("passenger_uuid", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("costCenter", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
]

# Schema for trip table.
schema_tripaction = [
    bigquery.SchemaField("passenger_uuid", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("costCenter", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("costCenter", "STRING", mode="NULLABLE"),
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

# Destination Tables
destination_table = 'data_engineering.tripaction'

project_id = 'turing-dev-337819'
bq_client = bigquery.Client(project=project_id)


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

df2 = pd.DataFrame()
for i, row in df.iterrows():
    for j in row['passengers']:
        temp_df_p = pd.json_normalize(row['passengers'])
        temp_df_p['uuid'] = row['uuid']
        df2 = df2.append(temp_df_p)

# temp_df = pd.json_normalize(resp['data'])
print(temp_df)

df_passengers = df2[[
    'person.email', 'person.costCenter', 'uuid']]
df_passengers.rename(columns={'person.uuid': 'passenger_uuid',
                              'person.name': 'name', 'person.costCenter': 'costCenter', 'person.email': 'email'}, inplace=True)

trips_df = df[['booker.name', 'travelMonth', 'bookingStatus', 'vendor', 'bookingType',
               'startDate', 'usdGrandTotal', 'saving', 'optimalPrice', 'paymentSchedule', 'paymentMethodUsed', 'tripDescription', 'inventory', 'purpose', 'tripName', 'uuid']]
trips_df.rename(columns={'booker.name': 'name'}, inplace=True)

trips_df[['startDate']] = trips_df[['startDate']].apply(
    pd.to_datetime, format=bq_date_format, utc=True)


df_final = pd.merge(trips_df, df_passengers, how='inner', on='uuid')

job_config = bigquery.LoadJobConfig(
    write_disposition='WRITE_TRUNCATE', autodetect=True, allow_quoted_newlines=True, schema=schema_tripaction)

load_job = bq_client.load_table_from_dataframe(
    df_final, destination_table, job_config=job_config)
