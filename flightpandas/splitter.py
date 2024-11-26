from flightpandas.flight import Flight
from flightpandas.flight_collection import FlightCollection
from flightpandas.helper_base import HelperBase

from pandas import Timedelta
from pandas.api.types import is_datetime64_any_dtype

def _validate_datetime_index(obj):
    if isinstance(obj, Flight):
        if not is_datetime64_any_dtype(obj.index):
            raise ValueError("Index must be a datetime64 type to use this splitter.\nUse `flight.index = pd.to_datetime(flight.index, ...)` to convert the index to datetime64.")
    elif isinstance(obj, FlightCollection):
        if not is_datetime64_any_dtype(obj.obj.index):
            raise ValueError("Index must be a datetime64 type to use this splitter\nUse `fc.data.index = pd.to_datetime(fc.data.index, ...)` to convert the index to datetime64.")
    return False


class TimeGapSplitter(HelperBase):
    """
    Split trajectories based on time gaps
    """
    def __init__(self, obj, gap=Timedelta(minutes=30), split_column_name='split'):
        _validate_datetime_index(obj)
        super().__init__(obj)
        self.gap = gap
        self.split_column_name = split_column_name

    def _eval_flight(self, flight: Flight) -> FlightCollection:
        data = flight.sort_index()
        t_diff = data.index.diff().fillna(Timedelta(0))
        data[self.split_column_name] = (t_diff > self.gap).cumsum()
        return data.groupby(self.split_column_name)

    def _eval_flight_collection(self, fc: FlightCollection) -> FlightCollection:
        group_index = fc.keys
        if not isinstance(group_index, list):
            group_index = [group_index]

        data = fc.data.copy()

        # TODO: Index removal already handled in FlightCollection init. Remove this later
        for index in group_index:
            try:
                data = data.reset_index(index)
            except KeyError:
                pass

        time_index = data.index.name

        # Sort the data by all index
        data = data.reset_index().set_index([*group_index, time_index]).sort_index()

        # Reindex with time index
        data = data.reset_index().set_index(time_index)

        # Perform the split
        t_diff = data.index.diff().fillna(Timedelta(0))
        data[self.split_column_name] = data.groupby([*group_index, (t_diff > self.gap).cumsum()]).ngroup()
        return data.groupby(self.split_column_name)
    
    