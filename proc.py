import inspect
from datetime import datetime
import pandas as pd
from pandas import DataFrame


# Custom Decorators
def assertion(func):
    """Decoractor to mark functions which handle data assertions"""
    func.is_assertion = True
    return func


def transformation(func):
    """Decorator to mark function which handle data transformations"""
    func.is_transformation = True
    return func

class Processor:
    """
    The Processor class serves as a two-step data handler.
    Firstly, it validates the data using assertions, ensuring its compliance with specified criteria.
    Secondly, it transforms the data to align with the schema of the database tables.
    """

    @classmethod
    def validate_with_assertions(cls, data):
        """Apply the implemented assertions on the data"""
        for _, method in inspect.getmembers(cls, predicate=lambda x: inspect.isfunction(x) or inspect.ismethod(x)):
            print("assert loop")
            if hasattr(method, "is_assertion"):
                data = method(data)
        return data

    @classmethod
    def transform_to_schema(cls, data):
        """Apply transformation on data to match database schema"""
        data = cls.add_timestamp(data)  # Ensure this runs first
        data = cls.add_speed(data)      # Ensure this runs after timestamp has been added

        data = cls.rename_columns(data)
        return {
            "breadcrumb_df": cls.get_breadcrumb_schema(data),
            "trip_df": cls.get_trip_schema(data),
        }

    
    @staticmethod
    @transformation
    def add_timestamp(df: DataFrame):
        """Replacing the OPD_DATE and ACT_TIME columns with a new TIMESTAMP column"""

        def _create_timestamp(df_row):
            """Helper function to create TIMESTAMP from OPD_DATE and ACT_TIME"""
            opd_date_timestamp = datetime.strptime(
                df_row["OPD_DATE"], "%d%b%Y:%H:%M:%S"
            )
            act_time_delta = pd.Timedelta(seconds=df_row["ACT_TIME"])
            return pd.Timestamp(opd_date_timestamp + act_time_delta)

        # add timestamp column
        df["TIMESTAMP"] = df.apply(_create_timestamp, axis=1)

        # drop unnecessary rows
        transformed_df = df.drop(["OPD_DATE", "ACT_TIME"], axis=1)
        return transformed_df

    @staticmethod
    @transformation
    def add_speed(df: DataFrame):
        """Creating a SPEED column computed from the METERS and TIMESTAMP columns"""

        df["dMETERS"] = df["METERS"].diff().fillna(0)
        df["dTIMESTAMP"] = df["TIMESTAMP"].diff().fillna(0)

        def _create_speed(df_row):
            """Helper function to create SPEED from dMETERS and dTIMESTAMP"""
            if df_row["dTIMESTAMP"] != 0:
                return df_row["dMETERS"] / df_row["dTIMESTAMP"].total_seconds()
            else:
                return 0

        # add speed column and ensure first row has same value as second row
        df["SPEED"] = df.apply(_create_speed, axis=1)
        df.loc[0, "SPEED"] = df.loc[1, "SPEED"]

        # drop unnecessary rows
        transformed_df = df.drop(["dMETERS", "dTIMESTAMP"], axis=1)
        return transformed_df

    @staticmethod
    def rename_columns(df: DataFrame):
        """Renames dataframe columns to match with database column names"""
        column_mapping = {
            "TIMESTAMP": "tstamp",
            "GPS_LATITUDE": "latitude",
            "GPS_LONGITUDE": "longitude",
            "SPEED": "speed",
            "EVENT_NO_TRIP": "trip_id",
            "VEHICLE_ID": "vehicle_id",
        }
        df.rename(columns=column_mapping, inplace=True)
        return df

    @staticmethod
    def get_breadcrumb_schema(df: DataFrame):
        """Get dataframe that fits the schema of the BreadCrumb table"""
        breadcrumb_table_columns = [
            "tstamp",
            "latitude",
            "longitude",
            "speed",
            "trip_id",
        ]
        return df[breadcrumb_table_columns]

    @staticmethod
    def get_trip_schema(df: DataFrame):
        """Get dataframe that fits the schema of the Trip table"""
        trip_table_columns = ["trip_id", "vehicle_id"]
        return df[trip_table_columns]

    @staticmethod
    @assertion
    def replace_missing(df: DataFrame):  # FIll missing data with nulls
        columns_replace = [
            "EVENT_NO_TRIP",
            "EVENT_NO_STOP",
            "OPD_DATE",
            "VEHICLE_ID",
            "METERS",
            "ACT_TIME",
            "GPS_LONGITUDE",
            "GPS_LATITUDE",
            "GPS_SATELLITES",
            "GPS_HDOP",
        ]
        for column in columns_replace:
            df = df.replace(f'"{column}": ,', f'"{column}": "NULL" ,')
        return df

    @staticmethod
    @assertion
    def vehicle_id_exist(df: DataFrame):
        """Validate data for missing vehicle IDs"""
        if "VEHICLE_ID" in df.columns:
            validated_df = df[df["VEHICLE_ID"] != "NULL"]
            print(f"Deleted {len(df) - len(validated_df)} rows with null 'Vehicle_ID'.")
            return validated_df

    @staticmethod
    @assertion
    def trip_id_exist(df: DataFrame):
        """Validate existance of trip ID"""
        if "EVENT_NO_TRIP" in df.columns:
            validated_df = df[df["EVENT_NO_TRIP"] != "NULL"]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with null 'EVENT_NO_TRIP'."
            )
            return validated_df

    @staticmethod
    @assertion
    def stop_id_exist(df: DataFrame):
        """Validate existance of stop ID"""
        if "EVENT_NO_STOP" in df.columns:
            validated_df = df[df["EVENT_NO_STOP"] != "NULL"]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with null 'EVENT_NO_STOP'."
            )
            return validated_df

    @staticmethod
    @assertion
    def lat_lon_intra(df: DataFrame):
        """Intra record check, if record has latitude it must have longitude and vice versa"""
        if "GPS_LATITUDE" in df.columns and "GPS_LONGITUDE" in df.columns:
            validated_df = df[
                ~(
                    ((df["GPS_LATITUDE"] == "NULL") & (df["GPS_LONGITUDE"] != "NULL"))
                    | ((df["GPS_LONGITUDE"] == "NULL") & (df["GPS_LATITUDE"] != "NULL"))
                )
            ]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with 'NULL' in either GPS Latitude or GPS Longitude"
            )
            return validated_df

    @staticmethod
    @assertion
    def lat_limit(df: DataFrame):
        """Check range of latitude (-90 to 90)"""
        if "GPS_LATITUDE" in df.columns:
            validated_df = df[(df["GPS_LATITUDE"] >= -90) & (df["GPS_LATITUDE"] <= 90)]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with a GPS_LATITUDE outside the legal range"
            )
            return validated_df

    @staticmethod
    @assertion
    def lon_limit(df: DataFrame):
        """Check range of longitude (-180 to 180)"""
        if "GPS_LONGITUDE" in df.columns:
            validated_df = df[
                (df["GPS_LONGITUDE"] >= -180) & (df["GPS_LONGITUDE"] <= 180)
            ]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with a GPS_LONGITUDE outside the legal range"
            )
            return validated_df

    @staticmethod
    @assertion
    def odom_summary(df: DataFrame):
        """Check to see if odomoter is ever jumping backwards (summary)"""
        if "METERS" in df.columns:
            validated_df = df[df["METERS"] >= df["METERS"].shift()]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with odometer readings less than the previous row."
            )
            return validated_df

    @staticmethod
    @assertion
    def odom_integrity(df: DataFrame):
        """Make sure odomoter is never negative"""
        if "METERS" in df.columns:
            validated_df = df[df["METERS"] >= 0]
            print(f"Deleted {len(df) - len(validated_df)} rows with negative 'METERS'.")
            return validated_df

    @staticmethod
    @assertion
    def gps_integrity(df: DataFrame):
        """Make sure satellite value is never below 0"""
        if "GPS_SATELLITES" in df.columns:
            validated_df = df[df["GPS_SATELLITES"] > 0]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with 'GPS SATELLITES' value 0 or below."
            )
            return validated_df

    @staticmethod
    @assertion
    def opd_exist(df: DataFrame):
        """Validate existance of OPD_DATE"""
        if "OPD_DATE" in df.columns:
            validated_df = df[df["OPD_DATE"] != "NULL"]
            print(f"Deleted {len(df) - len(validated_df)} rows with null 'OPD_DATE'.")
            return validated_df