# upload data to GCS
import os
from dotenv import load_dotenv, find_dotenv

# class ExtractRaw:
#     def __init__(self, **params):

#         self.params = params

# load env variables
load_dotenv(find_dotenv("./env"), verbose=True)

# setting up entsoe variables
security_token = os.environ.get("SECURITY_TOKEN")


def test_function():
    print(security_token)


test_function()
