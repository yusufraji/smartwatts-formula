import os
import threading
import time
from datetime import datetime

import openapi_client
from dotenv import load_dotenv
from openapi_client.api import carbon_aware_api


class Carbon:
    load_dotenv()

    def __init__(self, host="localhost", port=5073):
        self.host = host
        self.port = port
        self._configuration = self.generate_configuration()
        self.emission = None
        self.get_emissions_data_here_now()
        self._thread = threading.Thread(target=self.emission_loop)

    def generate_configuration(self):
        return openapi_client.Configuration(
            host=f"http://{self.host}:{self.port}",
            username=os.getenv("WATTTIME_USERNAME"),
            password=os.getenv("WATTTIME_PASSWORD"),
        )

    def get_emissions_data_here_now(self):
        api_response = None
        with openapi_client.ApiClient(self._configuration) as api_client:
            api_instance = carbon_aware_api.CarbonAwareApi(api_client)

            api_response = api_instance.get_emissions_data_for_location_by_time(
                location="westus",
                time=datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%MZ"),
                # async_req=True,
            )
        self.emission = api_response[0]["rating"]

    def emission_loop(self):
        while True:
            time.sleep(4 * 60)
