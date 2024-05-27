import pandas as pd
import json


class Processor:
    @classmethod
    def validate_with_assertions(cls, data):
        total_removed_rows = 0
        data, removed_rows = cls.vehicleIdExist(data)
        total_removed_rows += removed_rows
        data, removed_rows = cls.routeNumberExist(data)
        total_removed_rows += removed_rows
        data, removed_rows = cls.tripNumberExist(data)
        total_removed_rows += removed_rows
        data, removed_rows = cls.serviceKeyIntegrity(data)
        total_removed_rows += removed_rows
        data, removed_rows = cls.tripRouteIntegrity(data)
        total_removed_rows += removed_rows
        data, removed_rows = cls.serviceKeySummary(data)
        total_removed_rows += removed_rows
        if data is None:
            raise Exception("Attempted to validate empty or invalid data (e.g. misssing columns)")
        #print(f"Validation removed {total_removed_rows} rows in total", flush=True)
        return data

    @classmethod
    def transform_to_schema(cls, data):
        column_mapping = {
            "route_number": "route_id",
            "vehicle_number": "vehicle_id",
        }
        data.rename(columns=column_mapping, inplace=True)
        
        service_key_map = {
            "W" : "Weekday",
            "S" : "Saturday",
            "U" : "Sunday"
        }
        data["service_key"] = data["service_key"].replace(service_key_map)

        direction_key_map = {
            "0" : "Out",
            "1" : "Back"
        }
        data["direction"] = data["direction"].replace(direction_key_map)

        trip_table_columns = ["trip_id", "route_id", "vehicle_id", "service_key", "direction"]
        return data[trip_table_columns]

    # Load JSON file into a DataFrame
    @classmethod
    def load_json(cls, json_file):
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
    @staticmethod
    def vehicleIdExist(df):
        before_count = len(df)
        df = df[df['vehicle_number'].notnull()]
        after_count = len(df)
        removed_rows = before_count - after_count
        #print(f"vehicleIdExist removed {removed_rows} rows")
        return df, removed_rows

    # Existence Assertion: Route number cannot be null
    @staticmethod
    def routeNumberExist(df):
        before_count = len(df)
        df = df[df['route_number'].notnull()]
        after_count = len(df)
        removed_rows = before_count - after_count
        #print(f"routeNumberExist removed {removed_rows} rows")
        return df, removed_rows

    # Existence Assertion: trip_number cannot be null
    @staticmethod
    def tripNumberExist(df):
        before_count = len(df)
        df = df[df['trip_number'].notnull()]
        after_count = len(df)
        removed_rows = before_count - after_count
        #print(f"tripNumberExist removed {removed_rows} rows")
        return df, removed_rows

    # Integrity Assertion: The service_key portion of a record has to be a letter, no numbers allowed
    @staticmethod
    def serviceKeyIntegrity(df):
        before_count = len(df)
        df = df[df['service_key'].str.isalpha()]
        after_count = len(df)
        removed_rows = before_count - after_count
        #print(f"serviceKeyIntegrity removed {removed_rows} rows")
        return df, removed_rows

    # Intra-Record Check: If the record has a trip_id, then a route_id must exist as well (and vice versa)
    @staticmethod
    def tripRouteIntegrity(df):
        before_count = len(df)
        df = df[(df['trip_number'].notnull() & df['route_number'].notnull()) | (df['trip_number'].isnull() & df['route_number'].isnull())]
        after_count = len(df)
        removed_rows = before_count - after_count
        #print(f"tripRouteIntegrity removed {removed_rows} rows")
        return df, removed_rows

    # Summary Assertion: Fill empty service_key fields with the service key from the previous row
    @staticmethod
    def serviceKeySummary(df):
        before_count = len(df)
        df['service_key'] = df['service_key'].fillna(method='ffill')
        after_count = len(df)
        filled_rows = before_count - after_count
        #print(f"serviceKeySummary filled {filled_rows} rows with the service key from the previous row")
        return df, filled_rows
