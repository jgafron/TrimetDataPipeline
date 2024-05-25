# imports for Google's Pub/Sub
from google.cloud import pubsub_v1

# imports for api requests
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

# imports for parsing data
import json
from bs4 import BeautifulSoup


class Publisher:
    def __init__(self, project_id, topic_id, dataset_path):
        self.project_id = project_id
        self.topic_id = topic_id
        self.dataset_path = dataset_path
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish_stops_data(self, api_url):
        successful_api_calls = 0
        published_count = 0

        with open(self.dataset_path, mode="r") as file:
            dataset = json.load(file)

        for vehicle_id in dataset["vehicle_id"]:
            if (stops_data := self.get_stops_data(vehicle_id, api_url)) is None:
                continue

            successful_api_calls += 1
            soup = BeautifulSoup(stops_data, "html.parser")
            tables = soup.find_all("table")

            tables_data = []
            for table in tables:
                rows = table.find_all("tr")
                h_row = rows[0]
                headers = [th.get_text(strip=True) for th in h_row.find_all("th")]

                rows_data = []
                for row in rows[1:]:
                    if (cells := row.find_all("td")) == 0:
                        continue
                    cell_data = [cell.get_text(strip=True) for cell in cells]
                    rows_data.append(cell_data)

                tables_data.append({"headers": headers, "rows": rows_data})

            for data in tables_data:
                published_count += 1
                self.publish_data(data, published_count)

        return successful_api_calls, published_count

    def publish_breadcrumb_data(self, api_url):
        successful_api_calls = 0
        published_count = 0

        with open(self.dataset_path, mode="r") as file:
            dataset = json.load(file)

            for vehicle_id in dataset["vehicle_id"]:
                if (bc_data := self.get_breadcrumb_data(vehicle_id, api_url)) is None:
                    continue

                successful_api_calls += 1

                for data in bc_data:
                    published_count += 1
                    self.publish_data(data, published_count)

        return successful_api_calls, published_count

    def get_stops_data(self, vehicle_id, api_url):
        params = {"vehicle_num": vehicle_id}
        query_string = urlencode(params)
        url = f"{api_url}?{query_string}"

        try:
            with urlopen(url, timeout=10) as response:
                stops_data = response.read()
                return stops_data

        except HTTPError as error:
            print(error.status, error.reason, flush=True)
        except URLError as error:
            print(error.reason, flush=True)
        except TimeoutError:
            print("Request timed out", flush=True)

    def get_breadcrumb_data(self, vehicle_id, api_url):
        params = {"vehicle_id": vehicle_id}
        query_string = urlencode(params)
        url = f"{api_url}?{query_string}"

        try:
            with urlopen(url, timeout=10) as response:
                breadcrumb_data = json.load(response)
                return breadcrumb_data

        except HTTPError as error:
            print(error.status, error.reason, flush=True)
        except URLError as error:
            print(error.reason, flush=True)
        except TimeoutError:
            print("Request timed out", flush=True)

    def publish_data(self, data, count):
        json_data = json.dumps(data)
        byte_data = json_data.encode("utf-8")

        self.publisher.publish(self.topic_path, byte_data)
        if count % 10000 == 0:
            print(f"Published {count} messages so far to {self.topic_path}", flush=True)
