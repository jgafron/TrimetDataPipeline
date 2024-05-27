import os
import argparse
import sys
from subscriber import Subscriber
from bc_processor import Processor as BreadcrumbProcessor
from stops_processor import Processor as StopsProcessor

PROJECT_ID = "team-102-data-engineering"
BREADCRUMB_SUB_ID = "topic-102-sub"
STOPS_SUB_ID = "trimet-stop-events-sub"
TIMEOUT = 3

BASE_DIR = os.path.dirname(__file__)
BREADCRUMB_OUTPUT_DIR = os.path.join(BASE_DIR, "breadcrumb_output/")
STOPS_OUTPUT_DIR = os.path.join(BASE_DIR, "stops_output/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Subscriber can run in save mode or clear mode"
    )
    parser.add_argument(
        "-c", "--clear", action="store_true", help="Run subscriber in clear data mode"
    )
    args = parser.parse_args()

    breadcrumb_sub = Subscriber(
        project_id=PROJECT_ID,
        sub_id=BREADCRUMB_SUB_ID,
        timeout=TIMEOUT,
        processor=BreadcrumbProcessor,
    )
    stops_sub = Subscriber(
        project_id=PROJECT_ID,
        sub_id=STOPS_SUB_ID,
        timeout=TIMEOUT,
        processor=StopsProcessor,
    )
 
    if args.clear:
        breadcrumb_sub.clear_messages()
        stops_sub.clear_messages()
        sys.exit(1)
    
    # pipeline 1: Breadcrumb
    try:
        breadcrumb_sub.save_messages("Breadcrumb", BREADCRUMB_OUTPUT_DIR)
    finally:
        breadcrumb_sub.subscriber.close()
        print("Breadcrumb stream closed.", flush=True)

    # pipeline 2: Stops
    try:
        stops_sub.save_messages("Stops", STOPS_OUTPUT_DIR)
    finally:
        stops_sub.subscriber.close()
        print("Stops stream closed.", flush=True)
