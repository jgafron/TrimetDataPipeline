import json
import csv

# Function to process the JSON and generate a CSV
def process_json_to_csv(json_file, output_csv_file):
    # Open and load the JSON data
    with open(json_file, 'r') as file:
        data = json.load(file)

    # Prepare to write to a CSV file
    with open(output_csv_file, 'w', newline='') as csvfile:
        # Initialize a CSV writer
        csvwriter = None
        
        # Process each trip in the JSON data
        for trip in data:
            headers = trip['headers']
            rows = trip['rows']
            
            # Write headers to the CSV file if not already initialized
            if csvwriter is None:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(headers)
            
            # Only write the first row of each trip
            if rows:
                csvwriter.writerow(rows[0])

# Specify the input JSON file and the output CSV file
input_json_file = '2024-05-26_stops_data.json'  # Change this to your actual JSON file path
output_csv_file = '2024-05-26_stops.csv'

# Call the function with the file paths
process_json_to_csv(input_json_file, output_csv_file)
