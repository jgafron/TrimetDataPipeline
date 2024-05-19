import sys
import argparse

from pub import Publisher
from sub import Subscriber
from proc_1 import Processor

# configurations
PROJECT_ID = "team-102-data-engineering"
TOPIC_ID = "topic-102"
SUB_ID = "topic-102-sub"
BUS_DATA_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"
TIMEOUT = 300

if __name__ == "__main__":
    # checking for cli flags
    parser = argparse.ArgumentParser(description="Subscriber to Google Pub/Sub")
    parser.add_argument("-c", "--clear", action="store_true", help="Clear stream data")
    args = parser.parse_args()

    # instantiate publisher and subscriber
    breadcrumb_pub = Publisher(
        project_id=PROJECT_ID, topic_id=TOPIC_ID, api_url=BUS_DATA_URL
    )
    breadcrumb_sub = Subscriber(
        project_id=PROJECT_ID, sub_id=SUB_ID, processor=Processor, timeout=TIMEOUT
    )

    # clearing data from stream (-c or --clear flag)
    if args.clear:
        try:
            breadcrumb_sub.pull_messages(breadcrumb_sub.clear_messages, False)
        finally:
            breadcrumb_sub.subscriber.close()
            sys.exit(0)

    # publishing
    success, published = breadcrumb_pub.publish_breadcrumb_data()
    print(f"{success} successful API calls; {published} messages published", flush=True)

    # subscribing
    try:
        while True:  # run indefinitely
            breadcrumb_sub.pull_messages(breadcrumb_sub.log_messages, True)
    finally:
        breadcrumb_sub.subscriber.close()
