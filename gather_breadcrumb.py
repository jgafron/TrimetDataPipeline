import os
import json
from pprint import pprint
from datetime import datetime
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError, URLError

# constants
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data/')
DATASET_FILE_PATH = os.path.join(DATA_DIR, 'buttercup.json')
TEST_DATASET_FILE_PATH = os.path.join(DATA_DIR, 'test_data.json')
BUS_DATA_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"

def get_vehicle_data(url: str, vehicle_id: int) -> list | None:
    '''Sends a get request to an API to extract breadcrumb data by vehicle id'''
    # Parameters to include in the request
    params = {
        'vehicle_id': vehicle_id,
    }
    # Encode the parameters into a query string
    query_string = urlencode(params)

    # Construct the full URL with the query string
    full_url = f'{url}?{query_string}'

    try:
        with urlopen(full_url, timeout=10) as response:
            body = json.load(response)
        return body

    except HTTPError as error:
        print(error.status, error.reason)
    except URLError as error:
        print(error.reason)
    except TimeoutError:
        print('Request timed out')


def get_all_vehicle_data(dataset_file_path: str) -> None:
    '''Gets data from all vehicles in dataset and writes it into a json file'''
    # load in vehicle id from dataset
    with open(dataset_file_path, mode='r') as file:
        data = json.load(file)

    # we will append all data into this list and write it into the output file all at
    # once at the end of the function call
    output_data = []

    total_vehicle_ids = len(data["vehicle_ids"])
    failed_requests = 0
    for vehicle_id in data["vehicle_ids"]:
        response_data = get_vehicle_data(BUS_DATA_URL, vehicle_id=vehicle_id)
        if response_data:
            output_data.extend(response_data)
        else:
            failed_requests += 1
    print(f"{total_vehicle_ids - failed_requests} / {total_vehicle_ids} calls were succesful.")

    # The daily output file of bus data will be named with the date
    # Format the date as a string using the format 'YYYY-MM-DD'
    today = datetime.today()
    formatted_date = today.strftime('%Y-%m-%d')
    output_file_path = os.path.join(
        DATA_DIR, f'{formatted_date}_bus_data.json')

    # convert python list back to json
    json_object = json.dumps(output_data)

    # output json to json file
    absolute_output_file_path = os.path.join(os.getcwd(), output_file_path)
    with open(absolute_output_file_path, mode='w') as file:
        file.write(json_object)


if __name__ == "__main__":
    # For testing purposes only
    #print('''
    #    BUS DATA LOADER
    #    ---------------
    #    1 - Load test data set
    #    2 - Load full data set
    #''')
    #input(
    #"Please select from the options above by typing in the corresponding number: ")

    selection = '2'
    match selection:
        case '1':
            get_all_vehicle_data(TEST_DATASET_FILE_PATH)
        case '2':
            get_all_vehicle_data(DATASET_FILE_PATH)
        case _:
            print("Invalid response, please try again\n")