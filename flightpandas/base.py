class FlightPandasBase:
    _metadata = ["_altitude_column_name", "_altitude_rate_column_name", "_velocity_column_name", "_heading_column_name"]
    _altitude_column_name = None
    _altitude_rate_column_name = None
    _velocity_column_name = None
    _heading_column_name = None

    def get_altitude(self):
        if self._altitude_column_name is None:
            raise ValueError("altitude column is not set")
        return self[self._altitude_column_name]
    
    def get_altitude_rate(self):
        if self._altitude_rate_column_name is None:
            raise ValueError("altitude rate column is not set")
        return self[self._altitude_rate_column_name]
    
    def get_velocity(self):
        if self._velocity_column_name is None:
            raise ValueError("velocity column is not set")
        return self[self._velocity_column_name]
    
    def get_heading(self):
        if self._heading_column_name is None:
            raise ValueError("heading column is not set")
        return self[self._heading_column_name]
    