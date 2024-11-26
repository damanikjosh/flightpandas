import pandas
pandas.options.mode.copy_on_write = True

from flightpandas.flight import Flight
from flightpandas.flight_collection import FlightCollection

from flightpandas.splitter import TimeGapSplitter
from flightpandas.simplifier import RDP
