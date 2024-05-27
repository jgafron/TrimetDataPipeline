import os
import json
from datetime import datetime
from urllib import response
from google.cloud import pubsub_v1
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup

# Configurations
API_URL = "https://busdata.cs.pdx.edu/api/getStopEvents"
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data/")
DATASET_FILE_PATH = os.path.join(DATA_DIR, "buttercup.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "stops_data/")

def gather_data():
    tables_data = []
    with open(DATASET_FILE_PATH, mode="r") as file:
        data = json.load(file)

        success = 0
        published = 0

        for vehicle_id in data["vehicle_ids"]:
            response_data = get_stops_data(vehicle_id)
            if not response_data:
                continue

            success += 1

            soup = BeautifulSoup(response_data, "html.parser")
            tables = soup.find_all("table")

            for table in tables:
                rows = []
                table_rows = table.find_all("tr")
                header_row = table_rows[0]
                headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
                for row in table_rows[1:]:
                    cells = row.find_all("td")
                    if len(cells) == 0:
                        continue
                    data = [cell.get_text(strip=True) for cell in cells]
                    rows.append(data)
                table_data = {"headers": headers, "rows": rows}
                tables_data.append(table_data)

    save_data(tables_data)         
    return success

def save_data(data):
    json_data = json.dumps(data, indent=4)
    output_file = get_todays_file()
    output_file_path = os.path.join(OUTPUT_DIR, output_file)

    print(f"Writing to {output_file_path}", flush=True) 
    with open(output_file_path, "w") as file:
        file.write(json_data)


def get_stops_data(vehicle_id: int):
    """
    Utility function to gets data from breadcrumbs endpoint for a specific vehicle based on vehicle_id
    Returns a list of dicts containing data specific to a vehicle if the call was successful, otherwise None
    """
    params = {"vehicle_num": vehicle_id}
    query_string = urlencode(params)
    url = f"{API_URL}?{query_string}"

    body = None
    try:
        with urlopen(url, timeout=10) as response:
            body = response.read()
            return body

    except HTTPError as error:
        print(error.status, error.reason, flush=True)
    except URLError as error:
        print(error.reason, flush=True)
    except TimeoutError:
        print("Request timed out", flush=True)

def get_todays_file() -> str:
    """Utility function to create file name based on today's date"""
    today = datetime.today()
    formatted_date = today.strftime("%Y-%m-%d")
    data_file_path = f"{formatted_date}_stops_data.json"
    return data_file_path

if __name__ == "__main__":
    success = gather_data()
    print(f"{success} successful API calls")
