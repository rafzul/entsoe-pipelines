import pytest
from plugins.scripts.extract_raw import ExtractRawData
import json
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv("local.env"))


class TestExtractRaw:
    def test_extract_raw_data(self):
        extract_raw_data = ExtractRawData().extract_raw_data
        json_data = extract_raw_data(
            metrics_label="total_generation",
            start="202101010000",
            end="202101010600",
            timezone="Europe/Berlin",
            country_code="DE_TENNET",
        )
        assert json.loads(json_data) == True
