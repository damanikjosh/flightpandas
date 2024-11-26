"""
flight_collection.py

This module provides the `FlightCollection` class, which represents a grouped collection of flights,
and related utility classes and methods to manipulate and analyze flight trajectory data.

Classes:
--------
- FlightCollection: Extends `DataFrameGroupBy` and provides additional functionality for handling
  collections of `Flight` objects.
- _CollectionIndexer: A utility class for indexing `FlightCollection` by group keys or indices.

Functions:
----------
- dtw_distance_matrix: Computes the Dynamic Time Warping (DTW) distance matrix for flight trajectories.
- get_linestring: Aggregates flight data into LineString geometries.
- resample: Resamples flight trajectories to a specified temporal resolution.
- set_precision: Sets the precision for geometric data.
- to_crs: Transforms the coordinate reference system of the flight collection.
- to_latlon: Converts coordinates to latitude and longitude (EPSG:4326).
- to_xy: Converts coordinates to projected coordinates (EPSG:3857).

Examples:
---------
# Create a FlightCollection instance from a DataFrame
df = pd.DataFrame(...)
collection = FlightCollection(df, keys="flight_id", lat="latitude", lon="longitude")

# Access a flight group
flight = collection.flights["flight_id_1"]

# Compute DTW distance matrix
dtw_matrix = collection.dtw_distance_matrix()
"""

from flightpandas.flight import Flight
from pandas import Series, concat
from pandas.core.groupby import GroupBy, DataFrameGroupBy
from pandas._typing import IndexLabel
from geopandas import GeoSeries
from collections.abc import (
    Callable,
    Hashable,
    Mapping,
    Iterator,
)
from typing import Union

_KeysArgType = Union[
    Hashable,
    list[Hashable],
    Callable[[Hashable], Hashable],
    list[Callable[[Hashable], Hashable]],
    Mapping[Hashable, Hashable],
]

class _CollectionIndexer:
    """
    A utility class for indexing `FlightCollection` by group keys or indices.

    Attributes:
    -----------
    collection : FlightCollection
        The parent `FlightCollection` object.

    Methods:
    --------
    __call__(key):
        Retrieves a `Flight` object by group key.
    __getitem__(key):
        Retrieves a `Flight` object by integer index.
    """
    def __init__(self, collection):
        """
        Initializes the indexer with the parent collection.

        Parameters:
        -----------
        collection : FlightCollection
            The parent `FlightCollection` object.
        """
        self.collection = collection

    def __call__(self, key) -> Flight:
        return self.collection.get_group(key)
    
    def __getitem__(self, key) -> Flight:
        if not isinstance(key, int):
            raise ValueError("Only integer indexing is supported\nTo get a group by key, use `fc.flights(key)`")
        indices = list(self.collection.indices.keys())
        return self.collection.get_group(indices[key])
    

class FlightCollection(DataFrameGroupBy, GroupBy[Flight]):
    """
    Represents a grouped collection of `Flight` objects with additional methods for geospatial 
    and temporal operations.

    Attributes:
    -----------
    key_names : list
        The names of the keys used for grouping the collection.
    data : Flight
        The underlying flight data as a `Flight` object.

    Methods:
    --------
    __iter__():
        Iterates over the groups in the collection.
    dtw_distance_matrix(include_altitude=False, **kwargs):
        Computes the DTW distance matrix for the collection.
    get_linestring():
        Aggregates flight data into LineString geometries.
    resample(freq='1s', method='linear', **kwargs):
        Resamples the flight trajectories to a specified temporal resolution.
    set_precision(precision):
        Sets the precision for geometric data in the collection.
    to_crs(crs=None, epsg=None, **kwargs):
        Transforms the coordinate reference system of the collection.
    to_latlon():
        Converts coordinates to latitude and longitude (EPSG:4326).
    to_xy():
        Converts coordinates to projected coordinates (EPSG:3857).
    """

    def __init__(self, obj, keys=None, level=None, time=None, lat=None, lon=None, alt=None, alt_rate=None, velocity=None, heading=None, **kwargs):
        """
        Initializes a `FlightCollection` instance.

        Parameters:
        -----------
        obj : DataFrame or Flight
            The data to group into a `FlightCollection`.
        keys : str, list, Series, or None
            Keys or columns for grouping.
        level : int, str, or None
            Level in the index to use for grouping.
        time, lat, lon, alt, alt_rate, velocity, heading : str, optional
            Column names for flight attributes.
        **kwargs : dict
            Additional arguments for the constructor.

        Raises:
        -------
        ValueError:
            If neither `keys` nor `level` are provided.
        """
        if keys is None and level is None:
            raise ValueError("You have to supply one of 'keys' or 'level'")
        
        if not isinstance(obj, Flight):
            obj = Flight(obj, lat=lat, lon=lon, alt=alt, alt_rate=alt_rate, velocity=velocity, heading=heading)
        
        self.key_names = []
        if isinstance(keys, str):
            self.key_names = [keys]
        elif isinstance(keys, list):
            self.key_names = []
            for key in keys:
                if isinstance(key, Series):
                    self.key_names.append(key.name)
                elif isinstance(key, str):
                    self.key_names.append(key)
        elif isinstance(keys, Series):
            self.key_names = [keys.name]
        

        if isinstance(obj, Flight):
            for key_name in self.key_names:
                if key_name in obj.columns:
                    try:
                        obj = obj.reset_index(key_name)
                    except KeyError:
                        pass

        super().__init__(obj, keys=keys, level=level, **kwargs)


    def __iter__(self) -> Iterator[tuple[Hashable, Flight]]:
        """
        Iterates over the groups in the collection.

        Yields:
        -------
        tuple[Hashable, Flight]:
            A tuple containing the group key and the corresponding `Flight` object.
        """
        return super().__iter__()

    def _gotitem(self, key, ndim: int, subset=None):
        if ndim == 2:
            if subset is None:
                subset = self.obj
            if subset._geometry_column_name not in key:
                return super()._gotitem(key, ndim, subset)
            return FlightCollection(
                subset,
                self.keys,
                level=self.level,
                grouper=self._grouper,
                exclusions=self.exclusions,
                selection=key,
                as_index=self.as_index,
                sort=self.sort,
                group_keys=self.group_keys,
                observed=self.observed,
                dropna=self.dropna,
            )
        
        return super()._gotitem(key, ndim, subset)

    @property
    def data(self):
        return self.obj
    
    @property
    def flights(self):
        return _CollectionIndexer(self)
    
    def dtw_distance_matrix(self, include_altitude=False, **kwargs):
        """
        Computes the Dynamic Time Warping (DTW) distance matrix for the flight trajectories.

        Parameters:
        -----------
        include_altitude : bool, optional
            If True, includes altitude in the distance calculation. Defaults to False.
        **kwargs : dict
            Additional arguments for the DTW calculation.

        Returns:
        --------
        np.ndarray:
            The DTW distance matrix.
        """
        from dtaidistance import dtw_ndim

        print("Stacking series into a list")
        series_list = [flight.get_coordinates(include_altitude).to_numpy().copy() for _, flight in self]
        
        print("Calculating distance matrix")
        return dtw_ndim.distance_matrix_fast(series_list, **kwargs)
        
    def get_linestring(self) -> GeoSeries:
        """
        Aggregates flight data into LineString geometries.

        Returns:
        --------
        GeoSeries:
            A GeoSeries of LineString geometries for each flight group.
        """
        from shapely.geometry import LineString
        def _get_linestring(flight):
            if len(flight) < 2:
                return None
            return LineString(flight.get_coordinates())
                
        return self.aggregate({self.obj._geometry_column_name: _get_linestring})
    
    def resample(self, freq='1s', method='linear', **kwargs):
        """
        Resamples the flight trajectories to a specified temporal resolution.

        Parameters:
        -----------
        freq : str, optional
            The resampling frequency (e.g., '1T' for one minute). Defaults to '1s'.
        method : str, optional
            The interpolation method. Defaults to 'linear'.
        **kwargs : dict
            Additional arguments for interpolation.

        Returns:
        --------
        FlightCollection:
            A new `FlightCollection` instance with resampled trajectories.
        """
        data = self.data

        coordinates = data.get_coordinates().rename(columns={'x': 'lon', 'y': 'lat'})
        data_numeric = concat([coordinates, data.select_dtypes('number')], axis=1)
        print(list(data_numeric.columns))
        for key_name in self.key_names:
            if key_name not in data_numeric.columns:
                data_numeric[key_name] = data[key_name]
        data_nonnumeric = data.select_dtypes(exclude='number').drop(columns=data._geometry_column_name)
        for key_name in self.key_names:
            if key_name not in data_nonnumeric.columns:
                data_nonnumeric[key_name] = data[key_name]
        
        data_numeric = data_numeric.groupby(self.key_names)
        data_nonnumeric = data_nonnumeric.groupby(self.key_names)

        resampled_numeric = data_numeric.resample(freq).mean().interpolate(method, **kwargs)
        resampled_nonnumeric = data_nonnumeric.resample(freq).first().infer_objects(copy=False).bfill().ffill()
        # drop the key_names from the index of resampled_numeric
        for key_name in self.key_names:
            resampled_numeric = resampled_numeric.drop(columns=key_name)
            resampled_nonnumeric = resampled_nonnumeric.drop(columns=key_name)

        resampled = FlightCollection(concat([resampled_numeric, resampled_nonnumeric], axis=1), keys=self.key_names)
        for key_name in self.key_names:
            resampled.data.reset_index(key_name, inplace=True)
        return resampled
    
    def set_precision(self, precision) -> 'FlightCollection':
        """
        Sets the precision for geometric data in the collection.

        Parameters:
        -----------
        precision : int
            The desired precision for coordinates.

        Returns:
        --------
        FlightCollection:
            The updated `FlightCollection` instance.
        """
        def _set_precision(flight):
            flight[self.obj._geometry_column_name] = flight[self.obj._geometry_column_name].set_precision(precision)
            return flight
        return self.apply(lambda x: _set_precision(x)).groupby(self.key_names)
    
    def to_crs(self, crs=None, epsg=None, **kwargs) -> 'FlightCollection':
        """
        Transforms the coordinate reference system of the collection.

        Parameters:
        -----------
        crs : dict or str, optional
            The target CRS in dictionary or WKT format.
        epsg : int, optional
            The target CRS as an EPSG code.
        **kwargs : dict
            Additional arguments for the transformation.

        Returns:
        --------
        FlightCollection:
            The transformed `FlightCollection`.
        """
        return self.apply(lambda x: x.to_crs(crs, epsg, **kwargs)).groupby(self.key_names)
    
    def to_latlon(self):
        """
        Converts coordinates to latitude and longitude (EPSG:4326).

        Returns:
        --------
        FlightCollection:
            The updated collection in latitude/longitude format.
        """
        return self.to_crs(epsg=4326)
    
    def to_xy(self):
        """
        Converts coordinates to projected coordinates (EPSG:3857).

        Returns:
        --------
        FlightCollection:
            The updated collection in projected coordinate format.
        """
        return self.to_crs(epsg=3857)
    
