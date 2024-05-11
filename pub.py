import os
import json
from google.cloud import pubsub_v1
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

# Configuration Variables
PROJECT_ID = "team-102-data-engineering"
TOPIC_ID = "topic-102"
BUS_DATA_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data/")
DATASET_FILE_PATH = os.path.join(DATA_DIR, "buttercup.json")


class Publisher:
    """
    The Publisher receives data from the Breadcrumb API and publishes that data
    to a data pipeline, utilizing Google Cloud's Pub/Sub, a Stream-Processing System
    """

    def __init__(self, project_id: str, topic_id: str):
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish_vehicle_data(self, dataset_file_path: str) -> tuple[int, int]:
        """
        Gets data from all vehicles in given dataset and publishes all vehicle data to data pipeline
        Returns number of succesful API calls and numbers of messages published
        """
        # get all vehicle ids from dataset
        with open(dataset_file_path, mode="r") as file:
            data = json.load(file)

        # used to keep track of how many API requests were successful
        # and how many messages were sent to publisher
        success = 0
        published = 0

        # call API for each vehicle id in the dataset
        for vehicle_id in data["vehicle_ids"]:
            response_data = self.get_vehicle_data(vehicle_id)

            # immediately publish received data to the data pipeline on successful API call
            if response_data:
                success += 1
                # publish each message in API response list individually
                for data in response_data:
                    published += 1
                    self.publish_data(published, data)

        return success, published

    def get_vehicle_data(self, vehicle_id: int) -> list | None:
        """
        Utility function to gets data for a specific vehicle based on vehicle_id
        Returns a list of dicts containing data specific to a vehicle if the call was successful, otherwise None
        """
        # construct API URL based on vehicle id
        params = {"vehicle_id": vehicle_id}  # parameters (just vehicle_id in this case)
        query_string = urlencode(params)  # encode parameters into query string
        url = f"{BUS_DATA_URL}?{query_string}"  # contruct parameterized API URL

        # call API with a GET request and return response
        body = None
        try:
            with urlopen(url, timeout=10) as response:
                body = json.load(response)

        # handle errors (e.g. internet down, wrong URL, server down, etc.)
        except HTTPError as error:
            print(error.status, error.reason, flush=True)
        except URLError as error:
            print(error.reason, flush=True)
        except TimeoutError:
            print("Request timed out", flush=True)

        # dictionary of data is nested in a list, so return the dictionary if list is not empty
        return body if body else None

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


if __name__ == "__main__":
    team_102_pub = Publisher(PROJECT_ID, TOPIC_ID)
    success, published = team_102_pub.publish_vehicle_data(DATASET_FILE_PATH)
    print(f"{success} successful API calls; {published} messages published", flush=True)
