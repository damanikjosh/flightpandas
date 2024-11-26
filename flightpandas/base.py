class FlightPandasBase:
    """
    A base class providing foundational methods and metadata for managing flight-related data 
    in the FlightPandas framework.

    Attributes:
        _metadata (list): A list of metadata attribute names associated with flight data, 
            including altitude, altitude rate, velocity, and heading.
        _altitude_column_name (str): The column name for altitude data. Default is None.
        _altitude_rate_column_name (str): The column name for altitude rate data. Default is None.
        _velocity_column_name (str): The column name for velocity data. Default is None.
        _heading_column_name (str): The column name for heading data. Default is None.
    """

    _metadata = ["_altitude_column_name", "_altitude_rate_column_name", "_velocity_column_name", "_heading_column_name"]
    _altitude_column_name = None
    _altitude_rate_column_name = None
    _velocity_column_name = None
    _heading_column_name = None

    def get_altitude(self):
        """
        Retrieve altitude data from the designated column.

        Returns:
            Series: The altitude data stored in the column specified by `_altitude_column_name`.

        Raises:
            ValueError: If `_altitude_column_name` is not set.
        """
        if self._altitude_column_name is None:
            raise ValueError("altitude column is not set")
        return self[self._altitude_column_name]
    
    def get_altitude_rate(self):
        """
        Retrieve altitude rate data from the designated column.

        Returns:
            Series: The altitude rate data stored in the column specified by `_altitude_rate_column_name`.

        Raises:
            ValueError: If `_altitude_rate_column_name` is not set.
        """
        if self._altitude_rate_column_name is None:
            raise ValueError("altitude rate column is not set")
        return self[self._altitude_rate_column_name]
    
    def get_velocity(self):
        """
        Retrieve velocity data from the designated column.

        Returns:
            Series: The velocity data stored in the column specified by `_velocity_column_name`.

        Raises:
            ValueError: If `_velocity_column_name` is not set.
        """
        if self._velocity_column_name is None:
            raise ValueError("velocity column is not set")
        return self[self._velocity_column_name]
    
    def get_heading(self):
        """
        Retrieve heading data from the designated column.

        Returns:
            Series: The heading data stored in the column specified by `_heading_column_name`.

        Raises:
            ValueError: If `_heading_column_name` is not set.
        """
        if self._heading_column_name is None:
            raise ValueError("heading column is not set")
        return self[self._heading_column_name]
    