import inspect
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
        for _, method in inspect.getmembers(cls, inspect.ismethod):
            if hasattr(method, "is_assertion"):
                data = method(data)
        return data

    @classmethod
    def transform_to_schema(cls, data):
        """Apply transformation on data to match database schema"""
        for _, method in inspect.getmembers(cls, inspect.ismethod):
            if hasattr(method, "is_transformation"):
                data = method(data)
        return data

    @assertion
    @staticmethod
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

    @assertion
    @staticmethod
    def vehicle_id_exist(df: DataFrame):
        """Validate data for missing vehicle IDs"""
        if "VEHICLE_ID" in df.columns:
            validated_df = df[df["VEHICLE_ID"] != "NULL"]
            print(f"Deleted {len(df) - len(validated_df)} rows with null 'Vehicle_ID'.")
            return validated_df

    @assertion
    @staticmethod
    def trip_id_exist(df: DataFrame):
        """Validate existance of trip ID"""
        if "EVENT_NO_TRIP" in df.columns:
            validated_df = df[df["EVENT_NO_TRIP"] != "NULL"]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with null 'EVENT_NO_TRIP'."
            )
            return validated_df

    @assertion
    @staticmethod
    def stop_id_exist(df: DataFrame):
        """Validate existance of stop ID"""
        if "EVENT_NO_STOP" in df.columns:
            validated_df = df[df["EVENT_NO_STOP"] != "NULL"]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with null 'EVENT_NO_STOP'."
            )
            return validated_df

    @assertion
    @staticmethod
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

    @assertion
    @staticmethod
    def lat_limit(df: DataFrame):
        """Check range of latitude (-90 to 90)"""
        if "GPS_LATITUDE" in df.columns:
            validated_df = df[(df["GPS_LATITUDE"] >= -90) & (df["GPS_LATITUDE"] <= 90)]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with a GPS_LATITUDE outside the legal range"
            )
            return validated_df

    @assertion
    @staticmethod
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

    @assertion
    @staticmethod
    def odom_summary(df: DataFrame):
        """Check to see if odomoter is ever jumping backwards (summary)"""
        if "METERS" in df.columns:
            validated_df = df[df["METERS"] >= df["METERS"].shift()]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with odometer readings less than the previous row."
            )
            return validated_df

    @assertion
    @staticmethod
    def odom_integrity(df: DataFrame):
        """Make sure odomoter is never negative"""
        if "METERS" in df.columns:
            validated_df = df[df["METERS"] >= 0]
            print(f"Deleted {len(df) - len(validated_df)} rows with negative 'METERS'.")
            return validated_df

    @assertion
    @staticmethod
    def gps_integrity(df: DataFrame):
        """Make sure satellite value is never below 0"""
        if "GPS_SATELLITES" in df.columns:
            validated_df = df[df["GPS_SATELLITES"] > 0]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with 'GPS SATELLITES' value 0 or below."
            )
            return validated_df

    @assertion
    @staticmethod
    def opd_exist(df: DataFrame):
        """Validate existance of OPD_DATE"""
        if "OPD_DATE" in df.columns:
            validated_df = df[df["OPD_DATE"] != "NULL"]
            print(f"Deleted {len(df) - len(validated_df)} rows with null 'OPD_DATE'.")
            return validated_df
