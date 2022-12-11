import os 
from dotenv import load_dotenv
import requests
from google.oauth2 import service_account

load_dotenv()

security_token = os.environ.get("SECURITY_TOKEN")
service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE")
credentials = service_account.Credentials.from_service_account_file(
    service_account_file
)

headers = {
    "Content-Type":"text/xml",
}

entsoe_url = f"https://transparency.entsoe.eu/api?securityToken={security_token}&documentType=A65&processType=A16&outBiddingZone_Domain=10YCZ-CEPS-----N&periodStart=201512312300&periodEnd=201612312300"
entsoe_data = requests.get(entsoe_url, headers=headers)

print(entsoe_data)
