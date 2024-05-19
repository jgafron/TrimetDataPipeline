import os
import json
from urllib import response
from google.cloud import pubsub_v1
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup

# Configurations
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data/")
DATASET_FILE_PATH = os.path.join(DATA_DIR, "buttercup.json")


class Publisher:
    """
    The Publisher receives data from the Breadcrumb API and publishes that data
    to a data pipeline, utilizing Google Cloud's Pub/Sub, a Stream-Processing System
    """

    def __init__(self, project_id: str, topic_id: str, api_url):
        self.project_id = project_id
        self.topic_id = topic_id
        self.api_url = api_url
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish_stops_data(
        self, dataset_file_path: str = DATASET_FILE_PATH
    ) -> tuple[int, int]:
        """
        Gets data from all vehicles in given dataset and publishes all vehicle data to data pipeline
        Returns number of succesful API calls and numbers of messages published
        """
        with open(dataset_file_path, mode="r") as file:
            data = json.load(file)

        success = 0
        published = 0

        for vehicle_id in data["vehicle_ids"]:
            response_data = self.get_stops_data(vehicle_id)
            if not response_data:
                continue

            success += 1

            soup = BeautifulSoup(response_data, "html.parser")
            tables = soup.find_all("table")
            tables_data = []

            for table in tables:
                rows = []
                table_rows = table.find_all("tr")
                header_row = table_rows[0]
                headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
                for row in table_rows[1:]:
                    cells = row.find_all("td")
                    if len(cells) == 0:
                        continue
                    data = [cell.get_text(strip=True) for cell in cells]
                    rows.append(data)
                table_data = {"headers": headers, "rows": rows}
                tables_data.append(table_data)

            for data in tables_data:
                published += 1
                self.publish_data(published, data)

        return success, published

    def publish_breadcrumb_data(
        self, dataset_file_path: str = DATASET_FILE_PATH, bs4_parse=False
    ) -> tuple[int, int]:
        """
        Gets data from all vehicles in given dataset and publishes all vehicle data to data pipeline
        Returns number of succesful API calls and numbers of messages published
        """
        with open(dataset_file_path, mode="r") as file:
            data = json.load(file)

        success = 0
        published = 0

        for vehicle_id in data["vehicle_ids"]:
            response_data = self.get_breadcrumb_data(vehicle_id)
            if not response_data:
                continue

            success += 1

            for data in response_data:
                published += 1
                self.publish_data(published, data)

        return success, published

    def get_stops_data(self, vehicle_id: int):
        """
        Utility function to gets data from breadcrumbs endpoint for a specific vehicle based on vehicle_id
        Returns a list of dicts containing data specific to a vehicle if the call was successful, otherwise None
        """
        params = {"vehicle_num": vehicle_id}
        query_string = urlencode(params)
        url = f"{self.api_url}?{query_string}"

        body = None
        try:
            with urlopen(url, timeout=10) as response:
                body = response.read()
                return body

        except HTTPError as error:
            print(error.status, error.reason, flush=True)
        except URLError as error:
            print(error.reason, flush=True)
        except TimeoutError:
            print("Request timed out", flush=True)

    def get_breadcrumb_data(self, vehicle_id: int):
        """
        Utility function to gets data from breadcrumbs endpoint for a specific vehicle based on vehicle_id
        Returns a list of dicts containing data specific to a vehicle if the call was successful, otherwise None
        """
        params = {"vehicle_id": vehicle_id}
        query_string = urlencode(params)
        url = f"{self.api_url}?{query_string}"

        body = None
        try:
            with urlopen(url, timeout=10) as response:
                body = json.load(response)
                return body

        except HTTPError as error:
            print(error.status, error.reason, flush=True)
        except URLError as error:
            print(error.reason, flush=True)
        except TimeoutError:
            print("Request timed out", flush=True)

    def publish_data(self, count: int, data: dict):
        """
        Utility function to publish a message to Google Cloud's data pipeline
        """
        # prep data to fit with Google Pub data schema
        json_data = json.dumps(data)  # converts to json
        byte_data = json_data.encode("utf-8")  # converts to bytestring

        # publish the data
        self.publisher.publish(self.topic_path, byte_data)

        # print notification for every 1000 messages
        if count % 1000 == 0:
            print(f"Published {count} messages so far to {self.topic_path}", flush=True)
