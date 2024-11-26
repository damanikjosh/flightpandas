"""
plotter.py

This module provides visualization tools for flight trajectory data using Matplotlib and Cartopy.
It includes the `FlightPlotter` class, which supports line plots and scatter plots for geospatial
data, with optional Cartopy integration for geographic projections and features.

Classes:
--------
FlightPlotter:
    A class for visualizing flight data through line plots and scatter plots, supporting both
    Cartopy-based geographic projections and standard Matplotlib axes.

Attributes:
-----------
USE_CARTOPY : bool
    Indicates whether Cartopy is installed and available for use.

Examples:
---------
# Sample usage
from flightpandas.plotter import FlightPlotter

# Create a FlightPlotter instance
plotter = FlightPlotter(data, figsize=(10, 6), color="blue", features=["LAND", "OCEAN"])

# Generate a line plot
line_plot = plotter.plot()

# Generate a scatter plot
scatter_plot = plotter.scatter()
"""

import warnings
from geopandas import GeoSeries
import matplotlib.pyplot as plt
try:
    import cartopy.feature as cfeature
    import cartopy.crs as ccrs
    USE_CARTOPY = True
except ImportError:
    warnings.warn("Cartopy is not installed. Projections and geospatial features will not be available.\n To enable these features, install cartopy (`pip install cartopy`).")
    USE_CARTOPY = False

class FlightPlotter:
    """
    A class for visualizing flight trajectory data using Matplotlib and Cartopy (if available).

    Attributes:
    -----------
    data : GeoDataFrame or DataFrame
        The input flight data containing geospatial or temporal information.
    args : tuple
        Additional positional arguments for plotting.
    kwargs : dict
        Additional keyword arguments for plotting, including `figsize`, `color`, `ax`, and others.
    use_cartopy : bool
        Indicates whether Cartopy is used for geospatial features and projections.
    figsize : tuple, optional
        The size of the figure for Matplotlib plots. Default is None.
    color : str, optional
        The color of the plotted lines or scatter points. Default is "black".
    ax : Axes, optional
        The Matplotlib or Cartopy axes for plotting. Default is None.
    features : list, optional
        A list of Cartopy features to include in the plot (e.g., "LAND", "OCEAN").
        Default is ['LAND', 'OCEAN'] if Cartopy is enabled.
    projection : Cartopy CRS or None, optional
        The Cartopy coordinate reference system (CRS) to use for geographic plots. Default is `ccrs.PlateCarree()`.

    Methods:
    --------
    plot():
        Creates a line plot of the flight trajectory data.
    scatter():
        Creates a scatter plot of the flight trajectory data.
    _plot_lines(tc):
        Internal method for rendering line plots using Matplotlib.
    _plot_scatter(tc):
        Internal method for rendering scatter plots using Matplotlib.
    """
    def __init__(self, data, *args, **kwargs):
        """
        Initializes a `FlightPlotter` instance.

        Parameters:
        -----------
        data : GeoDataFrame or DataFrame
            The input flight data containing geospatial or temporal information.
        *args : tuple
            Additional positional arguments for plotting.
        **kwargs : dict
            Additional keyword arguments, including:
                - figsize (tuple, optional): The figure size for Matplotlib plots.
                - color (str, optional): The color for plotted elements. Default is "black".
                - ax (Axes, optional): An existing Matplotlib or Cartopy axis. Default is None.
                - features (list, optional): Cartopy features to add to the plot (e.g., "LAND", "OCEAN").
                - projection (Cartopy CRS or None, optional): The CRS for Cartopy plots. Default is `ccrs.PlateCarree()`.
        """
        self.data = data
        self.args = args
        self.kwargs = kwargs
        self.use_cartopy = USE_CARTOPY

        self.figsize = kwargs.pop("figsize", None)
        self.color = kwargs.pop("color", "black")
        self.ax = kwargs.pop("ax", None)
        # Get the projection of self.ax, if the projection is not set, USE_CARTOPY will be False
        if self.ax is not None and 'projection' not in self.ax.__dict__:
            self.use_cartopy = False
        if self.use_cartopy:
            self.features = kwargs.pop("features", ['LAND', 'OCEAN'])
            self.projection = kwargs.pop("projection", ccrs.PlateCarree())
        else:
            self.features = []
            self.projection = kwargs.pop("projection", None)

    def plot(self):
        """
        Creates a line plot of the flight trajectory data.

        If Cartopy is available and the `ax` has a Cartopy projection, adds geographic features
        (e.g., land, ocean) to the plot.

        Returns:
        --------
        AxesSubplot:
            The Matplotlib axes with the rendered line plot.
        """
        if not self.ax:
            self.ax = plt.figure(figsize=self.figsize).add_subplot(1, 1, 1, projection=self.projection)

        if self.use_cartopy:
            for feature in self.features:
                self.ax.add_feature(cfeature.__getattribute__(feature))
        line_plot = self._plot_lines(self.data)

        return line_plot
    
    def scatter(self):
        """
        Creates a scatter plot of the flight trajectory data.

        If Cartopy is available and the `ax` has a Cartopy projection, adds geographic features
        (e.g., land, ocean) to the plot.

        Returns:
        --------
        AxesSubplot:
            The Matplotlib axes with the rendered scatter plot.
        """
        if not self.ax:
            self.ax = plt.figure(figsize=self.figsize).add_subplot(1, 1, 1, projection=self.projection)

        if self.use_cartopy:
            for feature in self.features:
                self.ax.add_feature(cfeature.__getattribute__(feature))
        scatter_plot = self._plot_scatter(self.data)

        return scatter_plot


    def _plot_lines(self, tc):
        """
        Internal method for rendering line plots of flight trajectories.

        Parameters:
        -----------
        tc : GeoDataFrame or GeoSeries
            The geospatial flight data.

        Returns:
        --------
        AxesSubplot:
            The Matplotlib axes with the rendered line plot.
        """
        line_gdf = tc.get_linestring()
        if not isinstance(line_gdf, GeoSeries):
            line_gdf = GeoSeries([line_gdf])

        return line_gdf.plot(
            ax=self.ax,
            color=self.color,
            *self.args,
            **self.kwargs
        )

    def _plot_scatter(self, tc):
        """
        Internal method for rendering scatter plots of flight trajectories.

        Parameters:
        -----------
        tc : GeoDataFrame or GeoSeries
            The geospatial flight data.

        Returns:
        --------
        AxesSubplot:
            The Matplotlib axes with the rendered scatter plot.
        """
        markersize = self.kwargs.pop("markersize", 1)
        return tc.geometry.plot(
            ax=self.ax,
            color=self.color,
            markersize=markersize,
            *self.args,
            **self.kwargs
        )
    