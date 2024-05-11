import os
import json
import argparse
import pandas as pd

from pandas import DataFrame
from typing import Callable
from dotenv import load_dotenv
from datetime import datetime
from sqlalchemy import create_engine
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

from proc import Processor


# Configuration Variables
PROJECT_ID = "team-102-data-engineering"
SUB_ID = "topic-102-sub"
TIMEOUT = 300
BASE_DIR = os.path.dirname(__file__)
OUTPUT_DIR = os.path.join(BASE_DIR, "output/")


class Subscriber:
    """
    The Subscriber listens for and receives data on the data pipeline
    utilizing Google Cloud's Pub/Sub, a Stream-Processing System
    """

    def __init__(self, project_id: str, sub_id: str, timeout: int | None = None):
        self.project_id = project_id
        self.sub_id = sub_id
        self.timeout = timeout
        self.subscriber = pubsub_v1.SubscriberClient()
        self.sub_path = self.subscriber.subscription_path(project_id, sub_id)
        self.received_count = 0
        self.messages: list[dict] = []

    def pull_messages(self, callback: Callable, will_save: bool) -> None:
        """Pulls listens for and pulls message from data pipeline"""
        stream = self.subscriber.subscribe(self.sub_path, callback=callback)
        print(f"Listening for messages on {self.sub_path}..\n", flush=True)

        try:
            stream.result(timeout=TIMEOUT)
        # times out every 300 seconds to write collected data to output file
        except TimeoutError:
            stream.cancel()  # Trigger the shutdown.
            stream.result()  # Block until the shutdown is complete.

        # if not running cleaning mode, write messages to json file
        print(len(self.messages), flush=True)
        if will_save and self.messages:
            validated_data = self.validate_data()
            transformed_data = self.transform_data(validated_data)
            self.upload_to_db(transformed_data)
            self.save_messages()

    def validate_data(self):
        """Utility function to validate data received from data pipeline"""
        df = pd.DataFrame(self.messages)
        return Processor.validate_with_assertions(df)

    def transform_data(self, df):
        """Utility function to validate data received from data pipeline"""
        return Processor.transform_to_schema(df)

    def upload_to_db(self, data: dict[str, DataFrame]):
        """Utility function to upload data to PostgreSQL database"""
        load_dotenv()
        USERNAME = os.environ["USERNAME"]
        PASSWORD = os.environ["PASSWORD"]
        HOST = os.environ["HOST"]
        PORT = os.environ["PORT"]
        DB_NAME = os.environ["DB_NAME"]

        # establish connection to database
        engine = create_engine(
            f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
        )

        # append dataframe data to existing tables in Postgresql
        data["breadcrumb_df"].to_sql(
            "BreadCrumb", engine, if_exists="append", index=False
        )
        data["trip_df"].to_sql("Trip", engine, if_exists="append", index=False)

    def save_messages(self):
        """Utility function to save messages received to a json file"""
        # get file name by today's date
        output_file = self.get_todays_file()
        output_file_path = os.path.join(OUTPUT_DIR, output_file)

        # if json file already exists, load that data in so that we don't lose it
        if os.path.exists(output_file_path):
            with open(output_file_path, "r") as file:
                previously_loaded_messages = json.load(file)
            # combines previous messages with new one
            self.messages.extend(previously_loaded_messages)

        # format list to json format
        json_formatted_messages = json.dumps(self.messages, indent=4)

        # writes messages to output file
        print(f"Writing to {output_file_path}", flush=True)
        with open(output_file_path, "w") as file:
            file.write(json_formatted_messages)

        # clear message from memory to avoid rewriting duplicate entries
        self.messages = []

    def log_messages(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """Utility function that reads a message from the data stream and adds it to list of received messages"""
        # converts message from bytestring to utf-8
        data = message.data.decode("utf-8")

        # saves message to memory and removes it from the data pipeline
        self.messages.append(data)
        message.ack()

        # keep track of received messages and push notification every 1000 messages
        self.received_count += 1
        if self.received_count % 1000 == 0:
            print(f"Received {self.received_count} messages so far", flush=True)

    def clear_messages(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """Utility function to clear a message from the data stream"""
        message.ack()

    def get_todays_file(self) -> str:
        """Utility function to create file name based on today's date"""
        today = datetime.today()
        formatted_date = today.strftime("%Y-%m-%d")
        data_file_path = f"{formatted_date}_output_data.json"
        return data_file_path


if __name__ == "__main__":
    # checking for cli flags
    parser = argparse.ArgumentParser(description="Subscriber to Google Pub/Sub")
    parser.add_argument("-c", "--clear", action="store_true", help="Clear stream data")
    args = parser.parse_args()

    # initiliaze the Subscriber to appropriate project and topic
    team_102_sub = Subscriber(PROJECT_ID, SUB_ID, TIMEOUT)

    try:
        # clear the data stream if -c flag is set
        if args.clear:
            print("Clearing data stream", flush=True)
            team_102_sub.pull_messages(team_102_sub.clear_messages, False)

        # otherwise, listen for messages and writes them to json file every 5 minutes
        else:
            while True:  # run indefinitely
                team_102_sub.pull_messages(team_102_sub.log_messages, True)

    # close stream once operations above finish running
    finally:
        team_102_sub.subscriber.close()
