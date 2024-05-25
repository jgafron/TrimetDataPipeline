import os
import argparse
import sys
from subscriber import Subscriber
from bc_processor import Processor

PROJECT_ID = "data-eng-jtn7"
SUB_ID = "bus-data-sub"
TIMEOUT = 10

BASE_DIR = os.path.dirname(__file__)
BREADCRUMB_OUTPUT_DIR = os.path.join(BASE_DIR, "breadcrumb_output/")
STOPS_OUPUT_DIR = os.path.join(BASE_DIR, "stops_output/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Subscriber can run in save mode or clear mode"
    )
    parser.add_argument(
        "-c", "--clear", action="store_true", help="Run subscriber in clear data mode"
    )
    args = parser.parse_args()

    sub = Subscriber(
        project_id=PROJECT_ID,
        sub_id=SUB_ID,
        timeout=TIMEOUT,
        processor=Processor,
    )

    if args.clear:
        sub.clear_messages()
        sys.exit(1)

    try:
        while True:
            sub.save_messages("Breadcrumb", BREADCRUMB_OUTPUT_DIR)
    finally:
        sub.subscriber.close()
        print("Stream closed.", flush=True)
