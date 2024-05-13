import os
import json
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sub import Subscriber  # Ensure this is the correct import based on your file structure


PROJECT_ID = "team-102-data-engineering"
SUB_ID = "topic-102-sub"

def main():
    # Load environment variables
    load_dotenv()

    # Load test data
    with open('data/2024-05-11_bus_data.json', 'r') as file:
        test_data = json.load(file)

    # Initialize Subscriber
    sub = Subscriber(PROJECT_ID, SUB_ID)

    # Mock receiving messages
    sub.messages = test_data

    # Validate data
    validated_df = sub.validate_data()

    # Transform data
    transformed_data = sub.transform_data(validated_df)
    
    print(transformed_data)

    # Upload data to the database
    sub.upload_to_db(transformed_data)

    print("Data processing and upload complete.")

if __name__ == "__main__":
    main()
