# upload data to GCS
import os
import xmltodict
import json
import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage


class ExtractRaw:
    def __init__(self, **params):

        self.params = params

    # load env variables
    load_dotenv("./creds/.env", verbose=True, override=True)
    os.environ["TZ"] = "UTC"

    # setting up entsoe variables
    security_token = os.environ.get("SECURITY_TOKEN")

    # setting up GCP variables
    service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE")
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file
    )
    gcs_bucket = os.environ.get("CLOUD_STORAGE_BUCKET")

    def _upload_blob_to_gcs(bucket_name, credentials, contents, destination_blob_name):
        # Upload file to bucket"""

        # ID of GCS bucket
        # bucket_name =

        # the contents from memory to be uploaded to file
        # contents =

        # the ID of your GCS object
        # destination_blob_name =

        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(contents)

    def _extract_generation(
        entsoe_client,
        start,
        end,
        country_code,
    ):
        # Extract ENTSOE API for generation data
        # entsoe_client = entsoe RAW client
        # start = start date
        # end = end date
        # country_code = country code

        actual_start = start + pd.Timedelta(hours=1)
        actual_end = end + pd.Timedelta(hours=1)

        # create a client
        entsoe_data = entsoe_client.query_generation(
            country_code, start=actual_start, end=actual_end, psr_type=None
        )
        entsoe_dict = xmltodict.parse(entsoe_data)
        entsoe_json = json.dumps(entsoe_dict)
        return entsoe_json

    def extract_raw_data(
        metrics_label, bucket, start, end, timezone, country_code, **params
    ):
        # setting up entsoe client
        entsoe_client = EntsoeRawClient(api_key=security_token)

        # Extract ENTSOE API and upload to GCS
        start = pd.Timestamp(start, tz=timezone)
        end = pd.Timestamp(end, tz=timezone)
        country_code = country_code
        country_code_from = params.country_code_from
        country_code_to = params.country_code_to
        type_marketagreement_type = params.type_marketagreement_type
        contract_marketagreement_type = params.contract_marketagreement_type

        if metrics_label == "total_generation":
            entsoe_json_data = _extract_generation(
                entsoe_client, start, end, "", country_code
            )

        start_label = start.strftime("%Y%m%d%H%M")
        end_label = end.strftime("%Y%m%d%H%M%S")
        landing_filename = (
            f"{metrics_label}__{country_code}__{start_label}__{end_label}.json"
        )
        _upload_blob_to_gcs(bucket, credentials, entsoe_json_data, landing_filename)
