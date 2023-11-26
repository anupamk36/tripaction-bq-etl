import logging
import time
import argparse
from typing import Union
import requests

from google.cloud import secretmanager
from google.cloud import bigquery
import pandas as pd


from static import Constants

logging.basicConfig(level=logging.INFO)

# Function to get secrets from secret manager


def get_secret_data(project_id: str, secret_id: str, version_id: str) -> str:
    """get_secret_data returns the secret data requested.
    Args:
        project_id (str): Project ID associated with secret
        secret_id (str): Unique Secret ID of the secret
        version_id (str): Version of the secret
    Returns:
        response: JSON response
    """
    client = secretmanager.SecretManagerServiceClient()
    secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    response = client.access_secret_version(name=secret_detail)
    data = response.payload.data.decode("UTF-8")
    return data


# Function to generate token (life of token = 12h)
def get_token(client_id: str, client_secret: str) -> Union[str, None]:
    """get_token provides a token.

    Args:
        client_id (str): client_id
        client_secret (str): client_secret

    Returns:
        Union[str, None]: token
    """

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    response = requests.post(
        "https://api.tripactions.com/ta-auth/oauth/token", data=data, timeout=10
    )
    if response.status_code == 200:
        logging.info("Received token for tripaction")
        data = response.json()
        return "Bearer " + data["access_token"]
    logging.warning("Error while fetching token: %s", response.status_code)
    return None


# Runner function
def populate_tripaction_data(project_id: str) -> None:
    """populate_tripaction_data populates the tripaction data.

    Args:
        project_id (str): project id value
    """
    # loading job config and building client
    load_job_config = bigquery.QueryJobConfig(
        priority=bigquery.QueryPriority.BATCH)
    bq_client = bigquery.Client(
        project=project_id, default_query_job_config=load_job_config
    )

    # Secret values
    client_id = get_secret_data(
        project_id=project_id, secret_id=Constants.CLIENT_ID, version_id="latest"
    )
    client_secret = get_secret_data(
        project_id=project_id, secret_id=Constants.SECRET_ID, version_id="latest"
    )

    if token := get_token(client_id=client_id, client_secret=client_secret):
        response = None
        # Time in epoch format required
        start_epoch_time = 0
        current_epoch_time = int(time.time())

        headers = {
            "Authorization": token,
        }
        params = {
            "createdFrom": start_epoch_time,
            "createdTo": current_epoch_time,
            "page": "0",
            "size": "100",
        }

        response = requests.get(
            "https://api.tripactions.com/v1/bookings",
            params=params,
            headers=headers,
            timeout=15,
        )
        if response.status_code == 200:
            logging.info("Received response successfully")
            response = response.json()
            count = response["page"]["totalPages"]
            page = 0
            dataframe = pd.DataFrame()
            start_time = time.time()
            logging.info("ETL started")

            # Function to get complete data from API
            for page in range(count):
                headers = {
                    "Authorization": token,
                }
                params = {
                    "createdFrom": start_epoch_time,
                    "createdTo": current_epoch_time,
                    "page": page,
                    "size": "100",
                }

                response = requests.get(
                    "https://api.tripactions.com/v1/bookings",
                    params=params,
                    headers=headers,
                    timeout=15,
                )
                if response.status_code == 200:
                    logging.info(
                        "Received successful response for page: %s", page)
                    resp = response.json()
                    temp_df = pd.json_normalize(resp["data"])
                    if not temp_df["startDate"].isnull().any():
                        temp_df["travelMonth"] = pd.to_datetime(
                            temp_df["startDate"]
                        ).dt.strftime("%b-%y")
                        dataframe = pd.concat(
                            [dataframe, temp_df], ignore_index=True)
                    else:
                        logging.exception("Missing required value")

                else:
                    logging.warning("Incorrect response: %s",
                                    response.status_code)

            df_final = dataframe[
                [
                    "booker.name",
                    "booker.email",
                    "costCenters",
                    "travelMonth",
                    "tripName",
                    "usdGrandTotal",
                    "saving",
                    "optimalPrice",
                    "paymentSchedule",
                    "paymentMethodUsed",
                    "purpose",
                    "tripDescription",
                    "inventory",
                    "bookingStatus",
                    "startDate",
                    "vendor",
                    "bookingType",
                    "uuid",
                ]
            ]
            df_final.rename(
                columns={"booker.name": "name", "booker.email": "email"}, inplace=True
            )
            df_final[["startDate"]] = df_final[["startDate"]].apply(
                pd.to_datetime, format=Constants.BQ_DATE_FORMAT, utc=True
            )
            df_final["costCenters"] = (
                df_final["costCenters"].astype(str).str.extract(r"'([^']*)'")
            )
            if not df_final.empty:

                schema_tripaction = [
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "costCenters", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "travelMonth", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "bookingStatus", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("vendor", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "bookingType", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("startDate", "DATE", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "usdGrandTotal", "FLOAT64", mode="NULLABLE"),
                    bigquery.SchemaField("saving", "FLOAT64", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "optimalPrice", "FLOAT64", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "paymentSchedule", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "paymentMethodUsed", "STRING", mode="NULLABLE"
                    ),
                    bigquery.SchemaField(
                        "tripDescription", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "inventory", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("purpose", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField(
                        "tripName", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
                ]
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    autodetect=False,
                    allow_quoted_newlines=True,
                    schema=schema_tripaction,
                )

                load_job = bq_client.load_table_from_dataframe(
                    df_final, Constants.DESTINATION_TABLE, job_config=job_config
                )
                load_job.result()
                time_taken = time.strftime(
                    "%H:%M:%S", time.gmtime(time.time() - start_time)
                )
                logging.info("Time Taken in complete ETL: %s", time_taken)
                logging.info(
                    "ETL Finished, Loaded %s number of rows", len(
                        df_final.index)
                )
            else:
                logging.warning(
                    "Final dataframe has no records",
                )

        else:
            logging.warning("Incorrect response: %s", response.status_code)

    else:
        logging.exception("No token received")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p_id",
        "--project_id",
        dest="project_id",
        type=str,
        required=True,
        help="GCP Project ID",
    )
    args = parser.parse_args()
    # load tripaction data
    populate_tripaction_data(project_id=args.project_id)
