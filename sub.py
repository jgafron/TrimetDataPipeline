import os
import json
from datetime import datetime
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1


project_id = "team-102-data-engineering"
subscription_id = "topic-102-sub"
timeout = 300 # 5 minute timeout to write files every 5 minutes
base_dir = os.path.dirname(__file__)
output_dir = os.path.join(base_dir, "output/")

class Subscriber:
    '''Subscriber/Receiver of Google Pub/Sub, a Stream-Processing System'''
    def __init__(self, project_id: str, subscription_id: str, timeout: int=None):
        self.project_id = project_id
        self.subscription_id =subscription_id
        self.timeout = timeout
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)
        self.messages = []

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        '''Acknowledges messages to clear it from the datapipeline and adds messages to in memory list'''
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
            # write messages to json file
            save_messages()

    def save_messages(self):
        '''Saves messages received to a json file'''
        # get file name by today's date
        output_file = self.get_todays_file()
        output_file_path = os.path.join(output_dir, output_file)

        # if json file already exists, load that data in so that we don't lose it
        if os.path.exists(output_file_path):
            previously_loaded_messages = json.load(output_file_path)
            self.messages.extend(previously_loaded_messages)
        
        # format list to json format
        json_formatted_messages = json.dumps(self.messages, indent=4)

        # writes messages to output file
        print(f"Writing to {output_file_path}")
        with open(output_file_path, 'w') as file:
            file.write(json_formatted_messages)
        
        # clear message from memory to avoid rewriting duplicate entries
        self.messages = []

    def get_todays_file(self) -> str:
        '''Creates file name based on today's date'''
        today = datetime.today()
        formatted_date = today.strftime('%Y-%m-%d')
        data_file_path = f"{formatted_date}_output_data.json"
        return data_file_path


if __name__ == "__main__":
    sub = Subscriber(project_id, subscription_id, timeout)
    sub.pull_messages()
