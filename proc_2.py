class Processor:
    """
    The Processor class serves as a two-step data handler.
    Firstly, it validates the data using assertions, ensuring its compliance with specified criteria.
    Secondly, it transforms the data to align with the schema of the database tables.
    """

    @classmethod
    def validate_with_assertions(cls, data):
        """Apply the implemented assertions on the data"""
        if data is None:
            raise Exception(
                "Attempted to validate empty or invalid data(e.g. missing columns)"
            )
        return data

    @classmethod
    def transform_to_schema(cls, data):
        """Apply transformation on data to match database schema"""

        return data
