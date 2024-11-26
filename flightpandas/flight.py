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
    _metadata = FlightPandasBase._metadata + GeoDataFrame._metadata
    
    def __init__(self, data, *args, lat=None, lon=None, alt=None, alt_rate=None, velocity=None, heading=None, **kwargs):        
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
        self._set_attrs('altitude', other._altitude_column_name)
        self._set_attrs('altitude_rate', other._altitude_rate_column_name)
        self._set_attrs('velocity', other._velocity_column_name)
        self._set_attrs('heading', other._heading_column_name)
    
    def _set_attrs(self, name, value):
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

    
    def groupby(
        self,
        by=None,
        axis: Axis | lib.NoDefault = lib.no_default,
        level: IndexLabel | None = None,
        as_index: bool = True,
        sort: bool = True,
        group_keys: bool = True,
        observed: bool | lib.NoDefault = lib.no_default,
        dropna: bool = True,
    ):
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
        coordinates = super().get_coordinates()
        if include_altitude:
            coordinates['z'] = self[self._altitude_column_name]
        return coordinates
    
    def get_linestring(self):
        if len(self) < 2:
            return None
        from shapely.geometry import LineString
        return LineString(self.get_coordinates())
    
    def get_linestring_segment(self, start, end):
        from shapely.geometry import LineString
        return LineString(self.get_coordinates()[start:end])
    
    def set_precision(self, precision):
        self[self._geometry_column_name] = self[self._geometry_column_name].set_precision(precision)
        return self
    
    def dtw_distance(self, other, *args, **kwargs):
        from dtaidistance import dtw_ndim
        
        if not isinstance(other, Flight):
            raise ValueError("other must be a Flight instance")
        
        series1 = self.get_coordinates().to_numpy().copy()
        series2 = other.get_coordinates().to_numpy()

        return dtw_ndim.distance_fast(series1, series2, *args, **kwargs)
    
    def plot(self, *args, **kwargs):
        from flightpandas.plotter import FlightPlotter
        return FlightPlotter(self, *args, **kwargs).plot()
    
    def scatter(self, *args, **kwargs):
        from flightpandas.plotter import FlightPlotter
        return FlightPlotter(self, *args, **kwargs).scatter()
    
    def resample(self, freq='1s', method='linear', **kwargs):
        """
        Resample the flight trajectory to a specified frequency.

        Parameters
        ----------
        freq : str, optional
            The frequency to resample the trajectory to. This should be a
            string that can be parsed by pandas `resample` method, by default '1T'.

        Returns
        -------
        Flight
            A new Flight object with the resampled trajectory.

        Examples
        --------
        # Resample the flight trajectory to 5-minute intervals
        resampled_flight = flight.resample('5T')
        """
        coordinates = self.get_coordinates().rename(columns={'x': 'lon', 'y': 'lat'})
        data = concat([coordinates, self.select_dtypes('number')], axis=1)
        data_nonnumeric = self.select_dtypes(exclude='number').drop(columns=self._geometry_column_name)
        resampled = data.resample(freq).mean().interpolate(method, **kwargs)
        resampled_nonnumeric = data_nonnumeric.resample(freq).first().infer_objects(copy=False).bfill().ffill()
        resampled = Flight(concat([resampled, resampled_nonnumeric], axis=1), crs=self.crs)
        resampled._copy_attrs(self)
        return resampled
    