import os
from publisher import Publisher

PROJECT_ID = "team-102-data-engineering"
BREADCRUMB_TOPIC_ID = "topic-102"
BREADCRUMB_API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"
STOPS_TOPIC_ID = "trimet-stop-events"
STOPS_API_URL = "https://busdata.cs.pdx.edu/api/getStopEvents"

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data/")
DATASET_PATH = os.path.join(DATA_DIR, "buttercup.json")

if __name__ == "__main__":
    '''
    breadcrumb_pub = Publisher(
        project_id=PROJECT_ID,
        topic_id=BREADCRUMB_TOPIC_ID,
        dataset_path=DATASET_PATH,
    )
     
    bc_success, bc_published = breadcrumb_pub.publish_breadcrumb_data(BREADCRUMB_API_URL)
    print(f"Successful Breadcrumb API Calls: {bc_success}")
    print(f"Published Breadcrumb messages: {bc_published}")
    '''

    stops_pub = Publisher(
        project_id=PROJECT_ID,
        topic_id=STOPS_TOPIC_ID,
        dataset_path=DATASET_PATH,
    )
    stops_success, stops_published = stops_pub.publish_stops_data(STOPS_API_URL)
    print(f"Successful Stops API Calls: {stops_success}")
    print(f"Published Stops messages: {stops_published}")
