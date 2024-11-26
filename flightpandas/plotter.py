# -*- coding: utf-8 -*-
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
    def __init__(self, data, *args, **kwargs):
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
        if not self.ax:
            self.ax = plt.figure(figsize=self.figsize).add_subplot(1, 1, 1, projection=self.projection)

        if self.use_cartopy:
            for feature in self.features:
                self.ax.add_feature(cfeature.__getattribute__(feature))
        line_plot = self._plot_lines(self.data)

        return line_plot
    
    def scatter(self):
        if not self.ax:
            self.ax = plt.figure(figsize=self.figsize).add_subplot(1, 1, 1, projection=self.projection)

        if self.use_cartopy:
            for feature in self.features:
                self.ax.add_feature(cfeature.__getattribute__(feature))
        scatter_plot = self._plot_scatter(self.data)

        return scatter_plot


    def _plot_lines(self, tc):
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
        markersize = self.kwargs.pop("markersize", 1)
        return tc.geometry.plot(
            ax=self.ax,
            color=self.color,
            markersize=markersize,
            *self.args,
            **self.kwargs
        )
    