import os
from dotenv import load_dotenv
import requests
from google.oauth2 import service_account
import xmltodict
import json
from entsoe import EntsoeRawClient
import pandas as pd

# load env variables
load_dotenv()

security_token = os.environ.get("SECURITY_TOKEN")
service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE")
credentials = service_account.Credentials.from_service_account_file(
    service_account_file
)

headers = {
    "Content-Type": "text/xml",
}

# entsoe_url = f"https://transparency.entsoe.eu/api?securityToken={security_token}&documentType=A73&processType=A16&in_Domain=10YCZ-CEPS-----N&periodStart=202212141200&periodEnd=202212141300"
# entsoe_data = requests.get(entsoe_url, headers=headers)
# entsoe_dict = xmltodict.parse(entsoe_data.text)

# using entsoe library
client = EntsoeRawClient(api_key=security_token)
start = pd.Timestamp("202112141200", tz="Europe/Brussels")
end = pd.Timestamp("202112141300", tz="Europe/Brussels")
country_code = "BE"  # Belgium
country_code_from = "FR"  # France
country_code_to = "DE_LU"  # Germany-Luxembourg
type_marketagreement_type = "A01"
contract_marketagreement_type = "A01"

result = client.query_generation(country_code, start, end, psr_type=None)
entsoe_dict = xmltodict.parse(result)


with open("entsoe_data2.json", "w") as f:
    json.dump(entsoe_dict, f, indent=4)

print(entsoe_dict)

# open file with context manager
