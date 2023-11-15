# TripActions Data ETL to BigQuery

This Python script interfaces with the TripActions API to extract booking data and loads it into Google BigQuery for analytics and reporting. It leverages the `requests` library for API calls and `pandas` for data manipulation, along with Google Cloud's `secretmanager` and `bigquery` services for secure access to sensitive information and database operations, respectively.

## Features

- Secure retrieval of API credentials from Google Secret Manager.
- Extraction of booking data from the TripActions API.
- Data transformation to fit the BigQuery schema.
- Batch loading of data to a BigQuery table.

## Prerequisites

- Python 3.6 or higher.
- Access to Google Cloud Secret Manager and BigQuery services.
- TripActions API credentials and permissions.
- `google-cloud-secret-manager` and `google-cloud-bigquery` Python libraries.

## Installation

First, ensure that you have Python 3.6 or higher installed on your system. Then, install the required Python packages using the following command:

```bash
pip install -r requirements.txt
```
The requirements.txt should include:
```
requests
pandas
google-cloud-secret-manager
google-cloud-bigquery
```

## Configuration
Set up the following environment variables with your Google Cloud credentials:

- GOOGLE_APPLICATION_CREDENTIALS: Path to your Google Cloud service account key file.
- Ensure that the static.py file contains the necessary constants such as CLIENT_ID, SECRET_ID, and BQ_DATE_FORMAT used within the script.

## Usage
Run the script from the command line, providing the Google Cloud project ID as an argument:

```
python main.py --project_id YOUR_PROJECT_ID
```

## Functions
get_secret_data: Retrieves secret data from Google Secret Manager.
get_token: Authenticates with the TripActions API to obtain a bearer token.
populate_tripaction_data: Main function that orchestrates the ETL process.

## Logging
The script uses Python's built-in logging module to log its operations at the INFO level.

## Error Handling
The script includes error handling for HTTP errors and issues during data extraction, transformation, or loading.

## Contributing
Contributions to this project are welcome. Please ensure you adhere to the code style and add unit tests for any new or changed functionality.

## Author
Anupam Kumar

## Acknowledgments
Thanks to TripActions for their API.
Google Cloud services for secure and scalable data operations.
