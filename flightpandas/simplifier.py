from flightpandas.helper_base import HelperBase
from flightpandas.flight import Flight
from flightpandas.flight_collection import FlightCollection

try:
    from scipy.spatial import KDTree
    SCIPY_INSTALLED = True
except ImportError:
    SCIPY_INSTALLED = False

def _simplify_dataframe(df, simplified):
    if simplified is None:
        return df
    kdtree = KDTree(df.get_coordinates())
    indices = kdtree.query(simplified.coords)[1]
    return df.iloc[indices]
    
def _simplify_linestring(df, tolerance, preserve_topology):
    try:
        return df.get_linestring().simplify(tolerance, preserve_topology)
    except AttributeError:
        return None

class RDP(HelperBase):
    """
    Split trajectories based on time gaps
    """
    def __init__(self, obj, tolerance, preserve_topology=True, output_linestring=False):
        if not SCIPY_INSTALLED and output_linestring is False:
            raise ImportError("You need to install scipy to output dataframe.\nInstall it using `pip install scipy` or use `output_linestring=True` to output linestring.")
        
        super().__init__(obj)
        self.tolerance = tolerance
        self.preserve_topology = preserve_topology
        self.output_linestring = output_linestring

    def pipe(self, func, *args, **kwargs):
        if self.output_linestring:
            raise ValueError("Cannot apply further transformations after simplifying to a linestring.")
        return super().pipe(func, *args, **kwargs)

    def _eval_flight(self, flight: Flight) -> Flight:
        simplified = _simplify_linestring(flight, self.tolerance, self.preserve_topology)
        if self.output_linestring:
            return simplified
        return _simplify_dataframe(flight, simplified)

    def _eval_flight_collection(self, fc: FlightCollection) -> FlightCollection:
        simplified = fc.apply(lambda flight: self._eval_flight(flight))
        if self.output_linestring:
            return simplified
        return simplified.groupby(fc.key_names)
    