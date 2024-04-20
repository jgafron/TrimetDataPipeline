import os
import json
from datetime import datetime
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1


project_id = "team-102-data-engineering"
subscription_id = "topic-102-sub"
timeout = 3
base_dir = os.path.dirname(__file__)
output_dir = os.path.join(base_dir, "output/")

class Subscriber:
    def __init__(self, project_id: str, subscription_id: str, timeout: int=None):
        self.project_id = project_id
        self.subscription_id =subscription_id
        self.timeout = timeout
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)
        self.messages = []

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received {message}.")
        bytes_data = message.data
        data = bytes_data.decode("utf-8")
        self.messages.append(data)
        message.ack()
    
    def pull_messages(self):
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        print(f"Listening for messages on {self.subscription_path}..\n")

        with self.subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()  # Trigger the shutdown.
                streaming_pull_future.result()  # Block until the shutdown is complete.

    def save_messages(self, output_dir: str):
        output_file = self.get_todays_file()
        output_file_path = os.path.join(output_dir, output_file)
        json_formatted_messages = json.dumps(self.messages, indent=4)

        with open(output_file_path, 'w') as file:
            file.write(json_formatted_messages)
        
        self.messages = []

    def get_todays_file(self):
        today = datetime.today()
        formatted_date = today.strftime('%Y-%m-%d')
        data_file_path = f"{formatted_date}_output_data.json"
        return data_file_path


if __name__ == "__main__":
    sub = Subscriber(project_id, subscription_id, timeout)
    sub.pull_messages()
    sub.save_messages(output_dir)
