from datetime import datetime
import pandas as pd
from pandas import DataFrame


class Processor:
    """
    The Processor class serves as a two-step data handler.
    Firstly, it validates the data using assertions, ensuring its compliance with specified criteria.
    Secondly, it transforms the data to align with the schema of the database tables.
    """

    @classmethod
    def validate_with_assertions(cls, data):
        """Apply the implemented assertions on the data"""
        data = cls.replace_missing(data)
        data = cls.vehicle_id_exist(data)
        data = cls.trip_id_exist(data)
        data = cls.stop_id_exist(data)
        data = cls.lat_lon_intra(data)
        data = cls.lat_limit(data)
        data = cls.lon_limit(data)
        data = cls.odom_summary(data)
        data = cls.odom_integrity(data)
        data = cls.gps_integrity(data)
        data = cls.opd_exist(data)
        if data is None:
            raise Exception(
                "Attempted to validate empty or invalid data(e.g. missing columns)"
            )
        return data

    @classmethod
    def transform_to_schema(cls, data):
        """Apply transformation on data to match database schema"""
        data = cls.add_timestamp(data)  # Ensure this runs first
        data = cls.add_speed(data)  # Ensure this runs after timestamp has been added

        data = cls.rename_columns(data)
        return {
            "BreadCrumb": cls.get_breadcrumb_schema(data),
            "Trip": cls.get_trip_schema(data),
        }

    @staticmethod
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
        df.loc[1, "SPEED"] = df.loc[2, "SPEED"]

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
            "EVENT_NO_STOP": "event_no_stop",
            "METERS": "meters",
            "GPS_SATELLITES": "gps_satellites",
            "GPS_HDOP": "gps_hdop",
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
    def replace_missing(df: DataFrame):  # Fill missing data with nulls
        validated_df = df.fillna("NULL")
        return validated_df

    @staticmethod
    def vehicle_id_exist(df: DataFrame):
        """Validate data for missing vehicle IDs"""
        if "VEHICLE_ID" in df.columns:
            validated_df = df[df["VEHICLE_ID"] != "NULL"]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with null 'Vehicle_ID'.",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing VEHICLE_ID column")

    @staticmethod
    def trip_id_exist(df: DataFrame):
        """Validate existance of trip ID"""
        if "EVENT_NO_TRIP" in df.columns:
            validated_df = df[df["EVENT_NO_TRIP"] != "NULL"]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with null 'EVENT_NO_TRIP'.",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing EVENT_NO_TRIP column")

    @staticmethod
    def stop_id_exist(df: DataFrame):
        """Validate existance of stop ID"""
        if "EVENT_NO_STOP" in df.columns:
            validated_df = df[df["EVENT_NO_STOP"] != "NULL"]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with null 'EVENT_NO_STOP'.",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing EVENT_NO_STOP column")

    @staticmethod
    def lat_lon_intra(df: DataFrame):
        """Intra record check, if record has latitude it must have longitude and vice versa"""
        if "GPS_LATITUDE" in df.columns and "GPS_LONGITUDE" in df.columns:
            validated_df = df[
                (df["GPS_LATITUDE"] != "NULL") & (df["GPS_LONGITUDE"] != "NULL")
            ]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with 'NULL' in either GPS Latitude or GPS Longitude",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing GPS_LATITUDE OR GPS_LONGITUDE column")

    @staticmethod
    def lat_limit(df: DataFrame):
        """Check range of latitude (-90 to 90)"""
        if "GPS_LATITUDE" in df.columns:
            validated_df = df[(df["GPS_LATITUDE"] >= -90) & (df["GPS_LATITUDE"] <= 90)]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with a GPS_LATITUDE outside the legal range",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing GPS_LATITUDE column")

    @staticmethod
    def lon_limit(df: DataFrame):
        """Check range of longitude (-180 to 180)"""
        if "GPS_LONGITUDE" in df.columns:
            validated_df = df[
                (df["GPS_LONGITUDE"] >= -180) & (df["GPS_LONGITUDE"] <= 180)
            ]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with a GPS_LONGITUDE outside the legal range",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing GPS_LONGITUDE column")

    @staticmethod
    def odom_summary(df: DataFrame):
        """Check to see if odomoter is ever jumping backwards (summary)"""
        if "METERS" in df.columns:
            validated_df = df[df["METERS"] >= df["METERS"].shift()]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with odometer readings less than the previous row.",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing METERS column")

    @staticmethod
    def odom_integrity(df: DataFrame):
        """Make sure odomoter is never negative"""
        if "METERS" in df.columns:
            validated_df = df[df["METERS"] >= 0]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with negative 'METERS'.",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing METERS column")

    @staticmethod
    def gps_integrity(df: DataFrame):
        """Make sure satellite value is never below 0"""
        if "GPS_SATELLITES" in df.columns:
            validated_df = df[df["GPS_SATELLITES"] > 0]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with 'GPS SATELLITES' value 0 or below.",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing GPS_SATELLITES column")

    @staticmethod
    def opd_exist(df: DataFrame):
        """Validate existance of OPD_DATE"""
        if "OPD_DATE" in df.columns:
            validated_df = df[df["OPD_DATE"] != "NULL"]
            print(
                f"Deleted {len(df) - len(validated_df)} rows with null 'OPD_DATE'.",
                flush=True,
            )
            return validated_df
        raise Exception("Data missing OPD_DATE column")
