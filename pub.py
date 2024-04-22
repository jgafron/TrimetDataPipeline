import json
from datetime import datetime
from google.cloud import pubsub_v1


project_id = "team-102-data-engineering"
topic_id = "topic-102"

class Publisher:
    def __init__(self, project_id: str, topic_id: str):
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish_messages(self, data_file_path: str):
        # load json data
        with open(data_file_path, 'r') as file:
            json_data = json.load(file)

        # send each entry as a separate message
        for i, entry in enumerate(json_data):
            json_entry = json.dumps(entry)

            # data published must be bytestring
            data = json_entry.encode("utf-8")

            future = self.publisher.publish(self.topic_path, data)
            if i % 1000 == 0:
                print("Published 1000 messages")

        remaining_messages = len(json_data) % 1000
        print(f"Published {remaining_messages}")

        print(f"Published messages to {self.topic_path}.")

    def get_todays_data(self):
        today = datetime.today()
        formatted_date = today.strftime('%Y-%m-%d')
        data_file_path = f"data/{formatted_date}_bus_data.json"
        return data_file_path

if __name__ == "__main__":
    pub = Publisher(project_id, topic_id)
    todays_data_file = pub.get_todays_data()
    pub.publish_messages(todays_data_file)




