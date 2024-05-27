import pandas as pd
import json

# Load JSON file into a DataFrame
def load_json(json_file):
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            headers = data[0]['headers']
            rows = data[0]['rows']
            df = pd.DataFrame(rows, columns=headers)
        return df
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return None

# Existence Assertion: Vehicle_id cannot be null
def vehicleIdExist(df):
    before_count = len(df)
    df = df[df['vehicle_number'].notnull()]
    after_count = len(df)
    removed_rows = before_count - after_count
    print(f"vehicleIdExist removed {removed_rows} rows")
    return df, removed_rows

# Existence Assertion: Route number cannot be null
def routeNumberExist(df):
    before_count = len(df)
    df = df[df['route_number'].notnull()]
    after_count = len(df)
    removed_rows = before_count - after_count
    print(f"routeNumberExist removed {removed_rows} rows")
    return df, removed_rows

# Existence Assertion: trip_number cannot be null
def tripNumberExist(df):
    before_count = len(df)
    df = df[df['trip_number'].notnull()]
    after_count = len(df)
    removed_rows = before_count - after_count
    print(f"tripNumberExist removed {removed_rows} rows")
    return df, removed_rows

# Integrity Assertion: The direction column cannot be anything besides 0 or 1
def directionIntegrity(df):
    before_count = len(df)
    df = df[df['direction'].isin([0, 1])]
    after_count = len(df)
    removed_rows = before_count - after_count
    print(f"directionIntegrity removed {removed_rows} rows")
    return df, removed_rows

# Integrity Assertion: The service_key portion of a record has to be a letter, no numbers allowed
def serviceKeyIntegrity(df):
    before_count = len(df)
    df = df[df['service_key'].str.isalpha()]
    after_count = len(df)
    removed_rows = before_count - after_count
    print(f"serviceKeyIntegrity removed {removed_rows} rows")
    return df, removed_rows

# Intra-Record Check: If the record has a trip_id, then a route_id must exist as well (and vice versa)
def tripRouteIntegrity(df):
    before_count = len(df)
    df = df[(df['trip_number'].notnull() & df['route_number'].notnull()) | (df['trip_number'].isnull() & df['route_number'].isnull())]
    after_count = len(df)
    removed_rows = before_count - after_count
    print(f"tripRouteIntegrity removed {removed_rows} rows")
    return df, removed_rows

# Summary Assertion: Fill empty service_key fields with the service key from the previous row
def serviceKeySummary(df):
    before_count = len(df)
    df['service_key'] = df['service_key'].fillna(method='ffill')
    after_count = len(df)
    filled_rows = before_count - after_count
    print(f"serviceKeySummary filled {filled_rows} rows with the service key from the previous row")
    return df, filled_rows

# Entry point of the script
if __name__ == "__main__":
    json_file = "testdata.json"
    df = load_json(json_file)
    print(df)
    total_removed_rows = 0
    total_filled_rows = 0
    if df is not None:
        print("DataFrame loaded successfully:")
        print(df.head())
        df, removed_rows = vehicleIdExist(df)
        total_removed_rows += removed_rows
        df, removed_rows = routeNumberExist(df)
        total_removed_rows += removed_rows
        df, removed_rows = tripNumberExist(df)
        total_removed_rows += removed_rows
        df, removed_rows = directionIntegrity(df)
        total_removed_rows += removed_rows
        df, removed_rows = serviceKeyIntegrity(df)
        total_removed_rows += removed_rows
        df, removed_rows = tripRouteIntegrity(df)
        total_removed_rows += removed_rows
        df, filled_rows = serviceKeySummary(df)
        total_filled_rows += filled_rows
        print("Filtered DataFrame:")
        print(df.head())
        print(f"Total rows removed: {total_removed_rows}")
        print(f"Total rows filled with the service key from the previous row: {total_filled_rows}")

