 import EntsoeRawClient as entsoe)client
 
 class ExtractRawData:
    def __init__(self):
        # load env variables
        load_dotenv("/opt/airflow/.env", verbose=True)
        # setting up entsoe variables
        security_token = os.environ.get("ENTSOE_SECURITY_TOKEN")
        # setting up GCP variables
        gcs_bucket = os.environ.get("GCP_GCS_BUCKET")
        # setting up method map
        self.method_map = {
            "total_generation": "_extract_generation",
            "total_load": "_extract_load",
        }
        
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
 
    def extract_raw_data(metrics_label, start, end, timezone, country_code, **params):
        method_name = self.method_map.get(metrics_label, None)
            if method_name:
                method = getattr(self, method_name)
                entsoe_json_data = method(entsoe_client, start, end, country_code, **params)
            else:
                raise AttributeError(
                    f"No such extraction method for metrics_label: {metrics_label}"
                )
        print(entsoe_json_data)
                

if __name__ == "__main__":
    metrics_label = "total_generation",
    start = interval_start,
    end = interval_end,
    timezone = tz,
    country_code = country_code,
    ExtractRawData().extract_raw_data(metrics_label, start, end, timezone, country_code)