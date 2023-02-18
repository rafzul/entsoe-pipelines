# upload data to GCS
import os
import xmltodict
import json
import pandas as pd
from dotenv import load_dotenv, find_dotenv


# import GCS Hook (airflow only)
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# import EntsoeRawClient
from entsoe import EntsoeRawClient


# class ExtractRaw:
#     def __init__(self, **params):

#         self.params = params

# load env variables
load_dotenv("/opt/airflow/.env", verbose=True)
# setting up entsoe variables
security_token = os.environ.get("ENTSOE_SECURITY_TOKEN")
# setting up GCP variables
gcs_bucket = os.environ.get("GCP_GCS_BUCKET")

# class ExtractRawData:
#     def __init__(self):
#         # load env variables
#         load_dotenv("/opt/airflow/.env", verbose=True)
#         # setting up entsoe variables
#         security_token = os.environ.get("ENTSOE_SECURITY_TOKEN")
#         # setting up GCP variables
#         gcs_bucket = os.environ.get("GCP_GCS_BUCKET")
#         # setting up method map
#         self.method_map = {
#             "total_generation": "_extract_generation",
#             "total_load": "_extract_load",
#         }


def _upload_blob_to_gcs(bucket_name, contents, destination_blob_name):
    # Upload file to bucket"""

    # ID of GCS bucket
    # bucket_name =

    # the contents from memory to be uploaded to file
    # contents =

    # the ID of your GCS object
    # destination_blob_name =

    storage_client = GCSHook().get_conn()
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


def _extract_load(entsoe_client, start, end, country_code):
    actual_start = start + pd.Timedelta(hours=1)
    actual_end = end + pd.Timedelta(hours=1)

    # create a client
    entsoe_data = entsoe_client.query_load(
        country_code, start=actual_start, end=actual_end
    )

    entsoe_dict = xmltodict.parse(entsoe_data)
    entsoe_json = json.dumps(entsoe_dict)
    return entsoe_json


def _extract_day_ahead_prices(entsoe_client, start, end, country_code):
    actual_start = start + pd.Timedelta(hours=1)
    actual_end = end + pd.Timedelta(hours=1)

    # create a client
    entsoe_data = entsoe_client.query_day_ahead_prices(
        country_code, start=actual_start, end=actual_end
    )

    entsoe_dict = xmltodict.parse(entsoe_data)
    entsoe_json = json.dumps(entsoe_dict)
    return entsoe_json


def extract_raw_data(metrics_label, start, end, timezone, country_code, **params):
    # setting up entsoe client
    entsoe_client = EntsoeRawClient(api_key=security_token)
    # Extract ENTSOE API and upload to GCS
    start = pd.Timestamp(start, tz=timezone)
    end = pd.Timestamp(end, tz=timezone)
    country_code = country_code
    # country_code_from = params.country_code_from
    # country_code_to = params.country_code_to
    # type_marketagreement_type = params.type_marketagreement_type
    # contract_marketagreement_type = params.contract_marketagreement_type

    # method_name = self.method_map.get(metrics_label, None)
    # if method_name:
    #     method = getattr(self, method_name)
    #     entsoe_json_data = method(entsoe_client, start, end, country_code, **params)
    # else:
    #     raise AttributeError(
    #         f"No such extraction method for metrics_label: {metrics_label}"
    #     )
    if metrics_label == "total_generation":
        entsoe_json_data = _extract_generation(entsoe_client, start, end, country_code)
    elif metrics_label == "total_load":
        entsoe_json_data = _extract_load(entsoe_client, start, end, country_code)
    elif metrics_label == "day_ahead_prices":
        entsoe_json_data = _extract_day_ahead_prices(entsoe_client, start)
    else:
        pass

    start_label = start.strftime("%Y%m%d%H%M")
    end_label = end.strftime("%Y%m%d%H%M")
    landing_filename = (
        f"{metrics_label}__{country_code}__{start_label}__{end_label}.json"
    )
    _upload_blob_to_gcs(gcs_bucket, entsoe_json_data, landing_filename)
