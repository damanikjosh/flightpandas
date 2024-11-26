"""
splitter.py

This module provides tools to split flight trajectory data based on time gaps using the 
`TimeGapSplitter` class. The splitter is compatible with `Flight` and `FlightCollection` 
objects and supports custom time gap thresholds for trajectory segmentation.

Classes:
--------
- TimeGapSplitter: Splits flight trajectories into segments based on specified time gaps.

Functions:
----------
- _validate_datetime_index(obj): 
    Validates that the index of a `Flight` or `FlightCollection` object is of type datetime64.

Examples:
---------
# Split a flight based on time gaps
from flightpandas.splitter import TimeGapSplitter
splitter = TimeGapSplitter(flight, gap=pd.Timedelta(minutes=15))
segments = splitter.eval()

# Split a flight collection
splitter = TimeGapSplitter(flight_collection, gap=pd.Timedelta(hours=1))
collection_segments = splitter.eval()
"""
from flightpandas.flight import Flight
from flightpandas.flight_collection import FlightCollection
from flightpandas.helper_base import HelperBase

from pandas import Timedelta
from pandas.api.types import is_datetime64_any_dtype

def _validate_datetime_index(obj):
    """
    Validates that the index of a `Flight` or `FlightCollection` object is of type datetime64.

    Parameters:
    -----------
    obj : Flight | FlightCollection
        The flight data or collection to validate.

    Returns:
    --------
    bool:
        True if the index is valid, False otherwise.

    Raises:
    -------
    ValueError:
        If the index is not of type datetime64.
    """
    if isinstance(obj, Flight):
        if not is_datetime64_any_dtype(obj.index):
            raise ValueError("Index must be a datetime64 type to use this splitter.\nUse `flight.index = pd.to_datetime(flight.index, ...)` to convert the index to datetime64.")
    elif isinstance(obj, FlightCollection):
        if not is_datetime64_any_dtype(obj.obj.index):
            raise ValueError("Index must be a datetime64 type to use this splitter\nUse `fc.data.index = pd.to_datetime(fc.data.index, ...)` to convert the index to datetime64.")
    return False


class TimeGapSplitter(HelperBase):
    """
    Splits flight trajectories based on time gaps in the data.

    Attributes:
    -----------
    gap : Timedelta
        The minimum time gap to use for splitting trajectories.
    split_column_name : str
        The name of the column used to indicate the split segments.

    Methods:
    --------
    _eval_flight(flight):
        Splits a single `Flight` object based on time gaps.
    _eval_flight_collection(fc):
        Splits a `FlightCollection` object into groups based on time gaps.
    """

    def __init__(self, obj, gap=Timedelta(minutes=30), split_column_name='split'):
        """
        Initializes the TimeGapSplitter.

        Parameters:
        -----------
        obj : Flight | FlightCollection
            The flight data to split.
        gap : Timedelta, optional
            The time gap threshold for splitting trajectories. Default is 30 minutes.
        split_column_name : str, optional
            The column name for indicating split segments. Default is 'split'.

        Raises:
        -------
        ValueError:
            If the index of the provided object is not of type datetime64.
        """
        _validate_datetime_index(obj)
        super().__init__(obj)
        self.gap = gap
        self.split_column_name = split_column_name

    def _eval_flight(self, flight: Flight) -> FlightCollection:
        """
        Splits a single `Flight` object into segments based on time gaps.

        Parameters:
        -----------
        flight : Flight
            The flight trajectory data to split.

        Returns:
        --------
        FlightCollection:
            A collection of flight segments grouped by the split column.
        """
        data = flight.sort_index()
        t_diff = data.index.to_series().diff().fillna(Timedelta(0))
        data[self.split_column_name] = (t_diff > self.gap).cumsum()
        return data.groupby(self.split_column_name)

    def _eval_flight_collection(self, fc: FlightCollection) -> FlightCollection:
        """
        Splits a `FlightCollection` object into grouped segments based on time gaps.

        Parameters:
        -----------
        fc : FlightCollection
            The flight collection data to split.

        Returns:
        --------
        FlightCollection:
            A collection of segmented flight trajectories grouped by the split column.
        """
        group_index = fc.keys
        if not isinstance(group_index, list):
            group_index = [group_index]

        data = fc.data.copy()

        # Ensure the data is properly indexed
        for index in group_index:
            try:
                data = data.reset_index(index)
            except KeyError:
                pass

        time_index = data.index.name

        # Sort the data by group and time index
        data = data.reset_index().set_index([*group_index, time_index]).sort_index()

        # Reindex with the time index and calculate time differences
        data = data.reset_index().set_index(time_index)
        t_diff = data.index.to_series().diff().fillna(Timedelta(0))

        # Perform the split and assign split group numbers
        data[self.split_column_name] = data.groupby(
            [*group_index, (t_diff > self.gap).cumsum()]
        ).ngroup()
        return data.groupby(self.split_column_name)
    