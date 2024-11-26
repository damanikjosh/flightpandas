"""
simplifier.py

This module provides tools to simplify flight trajectory data using the Ramer-Douglas-Peucker (RDP) 
algorithm. It includes support for both `Flight` and `FlightCollection` objects and offers the 
ability to output either simplified dataframes or linestring geometries.

Classes:
--------
RDP:
    A helper class for simplifying flight trajectories using the RDP algorithm, with options 
    to preserve topology and output simplified linestrings.

Functions:
----------
_simplify_dataframe(df, simplified):
    Simplifies a DataFrame's flight trajectory using a KDTree for coordinate matching.

_simplify_linestring(df, tolerance, preserve_topology):
    Simplifies a LineString geometry with the specified tolerance and topology preservation.

Attributes:
-----------
SCIPY_INSTALLED : bool
    Indicates whether the SciPy library is installed.

Examples:
---------
# Simplify a flight trajectory with RDP
from flightpandas.simplifier import RDP
simplifier = RDP(flight, tolerance=0.01, preserve_topology=True, output_linestring=False)
simplified_flight = simplifier.eval()

# Simplify a flight collection
simplifier = RDP(flight_collection, tolerance=0.05, output_linestring=True)
simplified_collection = simplifier.eval()
"""

from flightpandas.helper_base import HelperBase
from flightpandas.flight import Flight
from flightpandas.flight_collection import FlightCollection

try:
    from scipy.spatial import KDTree
    SCIPY_INSTALLED = True
except ImportError:
    SCIPY_INSTALLED = False

def _simplify_dataframe(df, simplified):
    """
    Simplifies a DataFrame's flight trajectory using a KDTree for coordinate matching.

    Parameters:
    -----------
    df : DataFrame
        The original flight trajectory data as a DataFrame.
    simplified : shapely.geometry.LineString or None
        The simplified LineString geometry to match coordinates.

    Returns:
    --------
    DataFrame:
        A DataFrame containing the simplified trajectory.

    Raises:
    -------
    ImportError:
        If SciPy is not installed and `output_linestring` is set to False.
    """
    if simplified is None:
        return df
    kdtree = KDTree(df.get_coordinates())
    indices = kdtree.query(simplified.coords)[1]
    return df.iloc[indices]
    
def _simplify_linestring(df, tolerance, preserve_topology):
    """
    Simplifies a LineString geometry with the specified tolerance and topology preservation.

    Parameters:
    -----------
    df : GeoDataFrame or GeoSeries
        The flight trajectory data containing geospatial information.
    tolerance : float
        The tolerance for simplifying the geometry.
    preserve_topology : bool
        If True, preserves the topology of the geometry during simplification.

    Returns:
    --------
    shapely.geometry.LineString or None:
        The simplified LineString geometry, or None if simplification fails.
    """
    try:
        return df.get_linestring().simplify(tolerance, preserve_topology)
    except AttributeError:
        return None

class RDP(HelperBase):
    """
    Simplifies flight trajectories using the Ramer-Douglas-Peucker (RDP) algorithm.

    Attributes:
    -----------
    tolerance : float
        The tolerance for simplifying the trajectory.
    preserve_topology : bool
        If True, preserves the topology of the geometry during simplification.
    output_linestring : bool
        If True, outputs the simplified data as a LineString geometry. If False, outputs a DataFrame.

    Methods:
    --------
    pipe(func, *args, **kwargs):
        Adds a transformation to the pipeline. Raises an error if `output_linestring` is True.
    _eval_flight(flight):
        Applies RDP simplification to a single `Flight` object.
    _eval_flight_collection(fc):
        Applies RDP simplification to a `FlightCollection` object.

    Raises:
    -------
    ImportError:
        If SciPy is not installed and `output_linestring` is set to False.
    ValueError:
        If additional transformations are attempted after simplifying to a LineString.
    """

    def __init__(self, obj, tolerance, preserve_topology=True, output_linestring=False):
        """
        Initializes the RDP simplifier.

        Parameters:
        -----------
        obj : Flight | FlightCollection
            The flight data to simplify.
        tolerance : float
            The tolerance for simplifying the trajectory.
        preserve_topology : bool, optional
            If True, preserves the topology of the geometry during simplification. Default is True.
        output_linestring : bool, optional
            If True, outputs the simplified data as a LineString. Default is False.

        Raises:
        -------
        ImportError:
            If SciPy is not installed and `output_linestring` is set to False.
        """
        if not SCIPY_INSTALLED and output_linestring is False:
            raise ImportError("You need to install scipy to output dataframe.\nInstall it using `pip install scipy` or use `output_linestring=True` to output linestring.")
        
        super().__init__(obj)
        self.tolerance = tolerance
        self.preserve_topology = preserve_topology
        self.output_linestring = output_linestring

    def pipe(self, func, *args, **kwargs):
        """
        Adds a transformation to the pipeline. 

        Parameters:
        -----------
        func : callable
            The transformation function to apply.
        *args : tuple
            Additional positional arguments for the transformation function.
        **kwargs : dict
            Additional keyword arguments for the transformation function.

        Returns:
        --------
        HelperBase:
            The updated pipeline.

        Raises:
        -------
        ValueError:
            If additional transformations are attempted after simplifying to a LineString.
        """
        if self.output_linestring:
            raise ValueError("Cannot apply further transformations after simplifying to a linestring.")
        return super().pipe(func, *args, **kwargs)

    def _eval_flight(self, flight: Flight) -> Flight:
        """
        Applies RDP simplification to a single `Flight` object.

        Parameters:
        -----------
        flight : Flight
            The flight data to simplify.

        Returns:
        --------
        Flight | shapely.geometry.LineString:
            The simplified flight as a DataFrame or LineString, depending on `output_linestring`.
        """
        simplified = _simplify_linestring(flight, self.tolerance, self.preserve_topology)
        if self.output_linestring:
            return simplified
        return _simplify_dataframe(flight, simplified)

    def _eval_flight_collection(self, fc: FlightCollection) -> FlightCollection:
        """
        Applies RDP simplification to a `FlightCollection` object.

        Parameters:
        -----------
        fc : FlightCollection
            The flight collection to simplify.

        Returns:
        --------
        FlightCollection | shapely.geometry.LineString:
            The simplified flight collection as a grouped DataFrame or LineString, 
            depending on `output_linestring`.
        """
        simplified = fc.apply(lambda flight: self._eval_flight(flight))
        if self.output_linestring:
            return simplified
        return simplified.groupby(fc.key_names)
    