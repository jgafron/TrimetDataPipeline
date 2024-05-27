import pandas as pd
import json

# Load JSON file into a DataFrame
def load_json(json_file):
    try:
        with open(json_file, 'r') as f:
            data = f.read()
            data = replace_missing(data)
            df = pd.read_json(data)
        return df
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return None

# Replace missing values with 'NULL' in JSON data
def replace_missing(data):
    columns_replace = ["vehicle_number","leave_time", "train", "route_number","direction","service_key", "trip_number","stop_time","arrive_time","dwell","location_id","door","lift","ons",
                       "offs","estimated_load","maximum_speed","train_mileage","pattern_distance","location_distance","x_coordinate","y_coordinate","data_source", "schedule_status", "direction", "service_key"]
    for column in columns_replace:
        data = data.replace(f'"{column}": ,', f'"{column}": "NULL" ,')
    return data

# Existence Assertion: Vehicle_id cannot be null
def vehicleIdExist(df):
    assert 'vehicle_number' in df.columns and not df['vehicle_number'].isnull().any(), "Vehicle_id cannot be null"
    return df

# Existence Assertion: Route number cannot be null
def routeNumberExist(df):
    assert 'route_number' in df.columns and not df['route_number'].isnull().any(), "Route number cannot be null"
    return df

# Existence Assertion: trip_number cannot be null
def tripNumberExist(df):
    assert 'trip_number' in df.columns and not df['trip_number'].isnull().any(), "Trip number cannot be null"
    return df

# Integrity Assertion: The direction column cannot be anything besides 0 or 1
def directionIntegrity(df):
    assert 'direction' in df.columns and df['direction'].isin([0, 1]).all(), "Direction column must have values 0 or 1"
    return df

# Integrity Assertion: The service_key portion of a record has to be a letter, no numbers allowed
def serviceKeyIntegrity(df):
    assert 'service_key' in df.columns and df['service_key'].str.isalpha().all(), "Service key portion must be a letter, no numbers allowed"
    return df

# Intra-Record Check: If the record has a trip_id, than a route_id must exist as well (and vice versa)
def tripRouteIntegrity(df):
    assert all((df['trip_number'].notnull() & df['route_number'].notnull()) | (df['trip_number'].isnull() & df['route_number'].isnull())), "If the record has a trip_id, then a route_id must exist as well (and vice versa)"
    return df

# Summary Assertion: All records within a single days readings must have matching values for the service_key portion
def serviceKeySummary(df):
    assert df.groupby('date')['service_key'].nunique().eq(1).all(), "All records within a single day's readings must have matching values for the service_key portion"
    return df

# Entry point of the script
if __name__ == "__main__":
    json_file = "testdata.json"
    df = load_json(json_file)
    if df is not None:
        print("DataFrame loaded successfully:")
        print(df.head())
        df = vehicleIdExist(df)
        df = routeNumberExist(df)
        df = tripNumberExist(df)
        df = directionIntegrity(df)
        df = serviceKeyIntegrity(df)
        df = tripRouteIntegrity(df)
        df = serviceKeySummary(df)
        print("Assertions passed successfully.")
