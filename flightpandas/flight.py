"""
flight.py

This module defines the `Flight` class, a core component of the FlightPandas library, which 
specializes in managing, analyzing, and visualizing flight-related data using pandas and geopandas 
functionalities.

Classes:
--------
Flight:
    A class extending `FlightPandasBase` and `GeoDataFrame` for handling flight trajectory data 
    with geographic and temporal attributes. It includes methods for validation, metadata 
    management, geospatial operations, and advanced trajectory analysis.

Functions:
----------
_flight_constructor_with_fallback(*args, **kwargs):
    Attempts to create a `Flight` instance. Falls back to a regular pandas `DataFrame` 
    if the input is invalid.

_validate_attr(data, name, override=None, required=False):
    Validates and retrieves the appropriate column for a specified attribute from the data.

Variables:
----------
_possible_column_names: dict
    A dictionary of possible column names for latitude, longitude, altitude, altitude rate, 
    velocity, and heading to facilitate attribute detection.

Examples:
---------
# Sample DataFrame
data = {
    "time": pd.date_range("2024-01-01", periods=5, freq="H"),
    "lat": [10, 20, 30, 40, 50],
    "lon": [100, 110, 120, 130, 140],
}

df = pd.DataFrame(data)

# Create a Flight instance
flight = Flight(df, lat="lat", lon="lon")

# Access flight methods
resampled_flight = flight.resample('1T')
flight_dtw_distance = flight.dtw_distance(another_flight_instance)
flight.plot()
"""
import warnings

from flightpandas.base import FlightPandasBase

from geopandas import GeoDataFrame, points_from_xy
from geopandas.array import GeometryDtype
from pandas import DataFrame, concat
from pandas._typing import (
    Axis,
    IndexLabel,
)
from pandas._libs import lib

def _flight_constructor_with_fallback(*args, **kwargs):
    """
    Attempts to construct a `Flight` object. Falls back to a `DataFrame` if construction fails.

    Parameters:
        *args: Positional arguments for the constructor.
        **kwargs: Keyword arguments for the constructor.

    Returns:
        Flight or DataFrame: A `Flight` object if the input is valid; otherwise, a `DataFrame`.
    """
    try:
        df = Flight(*args, **kwargs)
    except ValueError:
        df = DataFrame(*args, **kwargs)
    return df

_possible_column_names = {
    'latitude': ['lat', 'latitude'],
    'longitude': ['lon', 'long', 'longitude'],
    'altitude': ['alt', 'altitude', 'baroaltitude', 'geoaltitude', 'baroalt', 'geoalt'],
    'altitude_rate': ['vertrate', 'alt_rate', 'baroalt_rate', 'geoalt_rate'],
    'velocity': ['velocity', 'groundspeed', 'spd', 'gs', 'speed'],
    'heading': ['heading', 'track', 'hdg', 'trk'],
}

def _validate_attr(data, name, override=None, required=False):
    """
    Validates and retrieves the appropriate column for a specified attribute from the data.

    Parameters:
        data (DataFrame or GeoDataFrame): The input data containing flight information.
        name (str): The attribute name to validate (e.g., 'latitude', 'altitude').
        override (str, optional): A specific column name to use. Defaults to None.
        required (bool, optional): If True, raises an error if the attribute is not found. Defaults to False.

    Returns:
        str: The validated column name for the attribute.

    Raises:
        ValueError: If the required attribute is not found in the data.
    """
    attr = getattr(data, f"_{name}_column_name") if hasattr(data, f"_{name}_column_name") else None
    if override is not None:
        attr = override
    elif attr is None:
        # Find in _possible_{name}_column_names
        for possible_name in _possible_column_names[name]:
            if possible_name in data.columns:
                print(f"Found {name} column: {possible_name}")
                return possible_name
            
    if attr not in data.columns:
        if required:
            raise ValueError(f"{name} is required")
        return None
    return attr


class Flight(FlightPandasBase, GeoDataFrame):
    """
    Represents flight trajectory data with support for geographic and temporal attributes. 
    Extends `FlightPandasBase` and `GeoDataFrame` for advanced data manipulation and analysis.

    Attributes:
        _metadata (list): Metadata attributes inherited from `FlightPandasBase` and `GeoDataFrame`.

    Methods:
        __init__: Initializes a `Flight` instance.
        _copy_attrs: Copies metadata attributes from another `Flight` instance.
        _set_attrs: Sets metadata attributes for the flight data.
        _constructor: Defines the constructor for `Flight` objects.
        _constructor_from_mgr: Constructs a `Flight` instance from a manager.
        _constructor_sliced_from_mgr: Sliced constructor for handling partial data.
        groupby: Groups the flight data by specified keys or levels.
        get_coordinates: Retrieves flight coordinates, optionally including altitude.
        get_linestring: Creates a LineString geometry from the flight coordinates.
        get_linestring_segment: Creates a LineString geometry for a specified segment.
        set_precision: Sets the precision for geometric data.
        dtw_distance: Computes the dynamic time warping distance between two flights.
        plot: Plots the flight trajectory.
        scatter: Creates a scatter plot of the flight trajectory.
        resample: Resamples the flight trajectory to a specified frequency.
    """
    _metadata = FlightPandasBase._metadata + GeoDataFrame._metadata
    
    def __init__(self, data, *args, lat=None, lon=None, alt=None, alt_rate=None, velocity=None, heading=None, **kwargs):
        """
        Initializes a `Flight` instance.

        Parameters:
            data (DataFrame or GeoDataFrame): The input data containing flight attributes.
            lat, lon (str, optional): Column names for latitude and longitude. Required for DataFrame input.
            alt, alt_rate, velocity, heading (str, optional): Column names for additional attributes.
            *args: Additional positional arguments for initialization.
            **kwargs: Additional keyword arguments for initialization.

        Raises:
            ValueError: If the input data is invalid.
        """
        if isinstance(data, Flight):
            # Copy attributes from another Flight instance
            # override attributes if specified
            alt = _validate_attr(data, 'altitude', data._altitude_column_name if alt is None else alt)
            alt_rate = _validate_attr(data, 'altitude_rate', data._altitude_rate_column_name if alt_rate is None else alt_rate)
            velocity = _validate_attr(data, 'velocity', data._velocity_column_name if velocity is None else velocity)
            heading = _validate_attr(data, 'heading', data._heading_column_name if heading is None else heading)
        elif isinstance(data, DataFrame):
            # lat, and lon are required
            # alt, alt_rate, velocity, and heading are optional
            lat = _validate_attr(data, 'latitude', lat, True)
            lon = _validate_attr(data, 'longitude', lon, True)
            alt = _validate_attr(data, 'altitude', alt)
            alt_rate = _validate_attr(data, 'altitude_rate', alt_rate)
            velocity = _validate_attr(data, 'velocity', velocity)
            heading = _validate_attr(data, 'heading', heading)

            data = GeoDataFrame(data.drop(columns=[lon, lat], axis=1), geometry=points_from_xy(data[lon], data[lat]), crs="EPSG:4326")
        elif isinstance(data, GeoDataFrame):
            alt = _validate_attr(data, 'altitude', alt)
            alt_rate = _validate_attr(data, 'altitude_rate', alt_rate)
            velocity = _validate_attr(data, 'velocity', velocity)
            heading = _validate_attr(data, 'heading', heading)
        else:
            raise ValueError("data must be a DataFrame or GeoDataFrame")
        
        super().__init__(data, *args, **kwargs)
        self._set_attrs('altitude', alt)
        self._set_attrs('altitude_rate', alt_rate)
        self._set_attrs('velocity', velocity)
        self._set_attrs('heading', heading)
    
    def _copy_attrs(self, other):
        """
        Copies metadata attributes from another `Flight` instance.

        Parameters:
            other (Flight): The source `Flight` instance.
        """
        self._set_attrs('altitude', other._altitude_column_name)
        self._set_attrs('altitude_rate', other._altitude_rate_column_name)
        self._set_attrs('velocity', other._velocity_column_name)
        self._set_attrs('heading', other._heading_column_name)
    
    def _set_attrs(self, name, value):
        """
        Sets a metadata attribute for the flight data.

        Parameters:
            name (str): The attribute name (e.g., 'altitude').
            value (str): The column name corresponding to the attribute.
        """
        setattr(self, f"_{name}_column_name", value)

    @property
    def _constructor(self):
        return _flight_constructor_with_fallback   
    
    def _constructor_from_mgr(self, mgr, axes):
        # TOOD: Change time column to index
        if not any(isinstance(block.dtype, GeometryDtype) for block in mgr.blocks):
            return _flight_constructor_with_fallback(DataFrame._from_mgr(mgr, axes))
        flight = Flight._from_mgr(mgr, axes)
        return flight
    
    def _constructor_sliced_from_mgr(self, mgr, axes):
        return super()._constructor_sliced_from_mgr(mgr, axes)
    
    def __getitem__(self, key):
        result = super(GeoDataFrame, self).__getitem__(key)

        if isinstance(result, Flight):
            return result
        else:
            return super().__getitem__(key)

    def __finalize__(self, other, method=None, **kwargs):
        """propagate metadata from other to self"""
        self = super().__finalize__(other, method=method, **kwargs)

        for name in self._metadata:
            attr = object.__getattribute__(self, name)

            # Expect only one column
            if (self.columns == attr).sum() != 1:
                object.__setattr__(self, name, None)

        return self

    
    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True, group_keys=True, observed=False, dropna=True):
        """
        Groups the flight data by specified keys or levels.

        Parameters:
            by (str or list, optional): Keys to group by. Defaults to None.
            axis (int, optional): The axis to group by. Defaults to 0.
            level (int or str, optional): The level to group by if using a MultiIndex. Defaults to None.
            as_index (bool, optional): Whether to return grouped data with indices. Defaults to True.
            sort (bool, optional): Whether to sort the group keys. Defaults to True.
            group_keys (bool, optional): Include group keys in the grouped data. Defaults to True.
            observed (bool, optional): For categorical data, observe the group keys. Defaults to False.
            dropna (bool, optional): Exclude groups with NaN keys. Defaults to True.

        Returns:
            FlightCollection: A grouped collection of flights.
        """
        if axis is not lib.no_default:
            axis = self._get_axis_number(axis)
            if axis == 1:
                warnings.warn(
                    "DataFrame.groupby with axis=1 is deprecated. Do "
                    "`frame.T.groupby(...)` without axis instead.",
                    FutureWarning
                )
            else:
                warnings.warn(
                    "The 'axis' keyword in DataFrame.groupby is deprecated and "
                    "will be removed in a future version.",
                    FutureWarning
                )
        else:
            axis = 0

        from flightpandas.flight_collection import FlightCollection
        
        if level is None and by is None:
            raise TypeError("You have to supply one of 'by' and 'level'")

        return FlightCollection(
            obj=self,
            keys=by,
            axis=axis,
            level=level,
            as_index=as_index,
            sort=sort,
            group_keys=group_keys,
            observed=observed,
            dropna=dropna,
        )
    
    def get_coordinates(self, include_altitude=False) -> list[tuple[float, float]]:
        """
        Retrieves the flight coordinates as a list of tuples.

        Parameters:
            include_altitude (bool, optional): If True, includes altitude in the coordinates. Defaults to False.

        Returns:
            list[tuple[float, float]]: A list of coordinates (and altitude if specified).
        """
        coordinates = super().get_coordinates()
        if include_altitude:
            coordinates['z'] = self[self._altitude_column_name]
        return coordinates
    
    def get_linestring(self):
        """
        Creates a LineString geometry from the flight coordinates.

        Returns:
            LineString: The LineString geometry of the flight trajectory.
        """
        if len(self) < 2:
            return None
        from shapely.geometry import LineString
        return LineString(self.get_coordinates())
    
    def get_linestring_segment(self, start, end):
        """
        Creates a LineString geometry for a specified segment of the flight trajectory.

        Parameters:
            start (int): The starting index of the segment.
            end (int): The ending index of the segment.

        Returns:
            LineString: The LineString geometry of the specified segment.
        """
        from shapely.geometry import LineString
        return LineString(self.get_coordinates()[start:end])
    
    def set_precision(self, precision):
        """
        Sets the precision for geometric data.

        Parameters:
            precision (int): The desired precision for the coordinates.

        Returns:
            Flight: The updated `Flight` instance.
        """
        self[self._geometry_column_name] = self[self._geometry_column_name].set_precision(precision)
        return self
    
    def dtw_distance(self, other, *args, **kwargs):
        """
        Computes the dynamic time warping (DTW) distance between two flights.

        Parameters:
            other (Flight): Another `Flight` instance to compare.
            *args: Additional positional arguments for DTW calculation.
            **kwargs: Additional keyword arguments for DTW calculation.

        Returns:
            float: The DTW distance between the two flights.

        Raises:
            ValueError: If the `other` object is not a `Flight` instance.
        """
        from dtaidistance import dtw_ndim
        
        if not isinstance(other, Flight):
            raise ValueError("other must be a Flight instance")
        
        series1 = self.get_coordinates().to_numpy().copy()
        series2 = other.get_coordinates().to_numpy()

        return dtw_ndim.distance_fast(series1, series2, *args, **kwargs)
    
    def plot(self, *args, **kwargs):
        """
        Plots the flight trajectory.

        Parameters:
            *args: Positional arguments for plotting.
            **kwargs: Keyword arguments for plotting.

        Returns:
            MatplotlibFigure: The resulting plot.
        """
        from flightpandas.plotter import FlightPlotter
        return FlightPlotter(self, *args, **kwargs).plot()
    
    def scatter(self, *args, **kwargs):
        """
        Creates a scatter plot of the flight trajectory.

        Parameters:
            *args: Positional arguments for the scatter plot.
            **kwargs: Keyword arguments for the scatter plot.

        Returns:
            MatplotlibFigure: The resulting scatter plot.
        """
        from flightpandas.plotter import FlightPlotter
        return FlightPlotter(self, *args, **kwargs).scatter()
    
    def resample(self, freq='1s', method='linear', **kwargs):
        """
        Resamples the flight trajectory to a specified frequency.

        Parameters:
            freq (str, optional): The resampling frequency (e.g., '1T' for 1 minute). Defaults to '1s'.
            method (str, optional): The interpolation method. Defaults to 'linear'.
            **kwargs: Additional keyword arguments for interpolation.

        Returns:
            Flight: A new `Flight` instance with the resampled trajectory.
        """
        coordinates = self.get_coordinates().rename(columns={'x': 'lon', 'y': 'lat'})
        data = concat([coordinates, self.select_dtypes('number')], axis=1)
        data_nonnumeric = self.select_dtypes(exclude='number').drop(columns=self._geometry_column_name)
        resampled = data.resample(freq).mean().interpolate(method, **kwargs)
        resampled_nonnumeric = data_nonnumeric.resample(freq).first().infer_objects(copy=False).bfill().ffill()
        resampled = Flight(concat([resampled, resampled_nonnumeric], axis=1), crs=self.crs)
        resampled._copy_attrs(self)
        return resampled
    