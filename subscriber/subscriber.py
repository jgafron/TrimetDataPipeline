import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from sqlalchemy import create_engine


class Subscriber:
    def __init__(self, project_id, sub_id, timeout, processor):
        self.project_id = project_id
        self.sub_id = sub_id
        self.timeout = timeout
        self.processor = processor
        self.subscriber = pubsub_v1.SubscriberClient()
        self.sub_path = self.subscriber.subscription_path(project_id, sub_id)
        self.messages = []
        self.count = 0
        self.init_db()

    def save_messages(self, dataset_name, output_dir):
        print("Pull messages from stream...", flush=True)
        try:
            self.pull_messages(self._save, dataset_name, output_dir)
        except Exception:
            print("Subscriber timeout...")

        print(f"Pulled {len(self.messages)} messages")
        if self.messages:
            print("Attempting to load messages to databse and save to json...", flush=True)
            data = self.process_data()
            self.upload_to_db(data)
            self.save_to_json(dataset_name, output_dir)

    def clear_messages(self):
        print("Clearing messages from stream...", flush=True)
        try:
            self.pull_messages(self._clear)
        except Exception:
            print("Subscriber timeout...")

    def pull_messages(self, callback, dataset_name=None, output_dir=None):
        stream = self.subscriber.subscribe(self.sub_path, callback=callback)
        print(f"Listening for messages on {self.sub_path}..\n", flush=True)

        try:
            stream.result(timeout=self.timeout)
        # times out every 300 seconds to write collected data to output file
        except TimeoutError:
            stream.cancel()  # Trigger the shutdown.
            stream.result()  # Block until the shutdown is complete.

    def _save(self, message):
        json_data = message.data.decode("utf-8")
        data = json.loads(json_data)
        self.messages.append(data)

        message.ack()
        self.count += 1
        if self.count % 10000 == 0:
            print(f"Received {self.count} messages so far...", flush=True)

    def _clear(self, message):
        message.ack()

    def process_data(self):
        json_messages = json.dumps(self.messages, indent=4)
        messages = json.loads(json_messages)
        df = pd.DataFrame(messages)
        validated_data = self.processor.validate_with_assertions(df)
        transformed_data = self.processor.transform_to_schema(validated_data)
        return transformed_data

    def upload_to_db(self, data):
        # append dataframe data to existing tables in Postgresql
        for table, dataframe in data.items():
            dataframe.to_sql(table, self.engine, if_exists="append", index=False)
            print(f"Loaded {len(dataframe)} rows of data to {table}")

    def init_db(self):
        load_dotenv()
        USERNAME = os.environ["USERNAME"]
        PASSWORD = os.environ["PASSWORD"]
        HOST = os.environ["HOST"]
        PORT = os.environ["PORT"]
        DB_NAME = os.environ["DB_NAME"]

        # establish connection to database
        self.engine = create_engine(
            f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
        )

    def save_to_json(self, dataset_name, output_dir):
        today = datetime.today()
        formatted_date = today.strftime("%Y-%m-%d")
        output_filename = f"{formatted_date}_{dataset_name}_data.json"
        output_path = os.path.join(output_dir, output_filename)

        if os.path.exists(output_path):
            with open(output_path, "r") as file:
                previously_loaded = json.load(file)
            self.messages.extend(previously_loaded)

        json_messages = json.dumps(self.messages, indent=4)
        print(f"Saving messages to {output_path}", flush=True)
        with open(output_path, "w") as file:
            file.write(json_messages)

        self.messages = []
