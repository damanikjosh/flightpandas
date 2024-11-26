# FlightPandas

> **Notice**: This library is still under heavy development. Features and APIs may change.

FlightPandas is a Python library for processing, analyzing, and visualizing flight trajectory data using Pandas and GeoPandas. It provides an intuitive interface for working with individual flights, grouped flight data, and time series operations.


## **Features**

- **Time Series Handling**: Resample and interpolate flight trajectory data efficiently.
- **Geospatial Support**: Seamless integration with GeoPandas for handling spatial data.
- **Flight Trajectory Representation**: Convert point data into LineString geometries.
- **Group Operations**: Manage and analyze grouped flight trajectories.
- **Custom Splitters**: Extend functionality with modules for splitting flights and observations.


## **Installation**

### **Using `pip`**
```bash
pip install flightpandas
```

### **From Source**
1. Clone the repository:
   ```bash
   git clone https://github.com/your_username/flightpandas.git
   cd flightpandas
   ```
2. Install the library:
   ```bash
   pip install .
   ```


## **Quick Start**

### **Basic Usage**

#### **Initialize a Flight**
```python
from flightpandas import FlightCollection, RDP
import pandas as pd

# Sample DataFrame
data = {
    "flight_id": [1, 1, 2, 2],
    "time": pd.date_range("2024-01-01", periods=4, freq="5min"),
    "lat": [10, 20, 30, 40],
    "lon": [100, 110, 120, 130],
}
df = pd.DataFrame(data).set_index("time")

# Create a FlightCollection
group = FlightCollection(df, keys="flight_id", time="time", lat="lat", lon="lon")

# Get the first flight
flight = group.flights[0]
print('Flight 0: ', flight)

# Resample the data
resampled = flight.resample(freq='10s', method='linear')
print('Flight 0 resampled: ', resampled)

# Simplify the first flight
simplified = RDP(resampled, tolerance=0.01).eval()
print('Flight 0 simplified: ', simplified)
```

#### **Working with Flight Collection**
```python
from flightpandas import FlightCollection, RDP
import pandas as pd

# Sample DataFrame
data = {
    "flight_id": [1, 1, 2, 2],
    "time": pd.date_range("2024-01-01", periods=4, freq="5min"),
    "lat": [10, 20, 30, 40],
    "lon": [100, 110, 120, 130],
}
df = pd.DataFrame(data).set_index("time")

# Create a FlightCollection
collection = FlightCollection(df, keys="flight_id", time="time", lat="lat", lon="lon")

# Resample the data
resampled = collection.resample(freq='10s', method='linear')

# Iterate over the resampled data
for key, flight in resampled:
    print(f"Key: {key}")
    print(flight)

```


## **Documentation**

The library is structured as follows:

### **Modules**
- **`base.py`**: Core functionality for trajectory data management.
- **`flight.py`**: Methods for individual flight trajectory analysis.
- **`flight_group.py`**: Operations on grouped flight trajectories.
- **`plotter.py`**: 
- **`simplifier.py`**:
- **`splitter`**: Utilities for splitting flight data based on criteria.

### **Key Classes**
- **`Base`**: Foundation for managing trajectory data.
- **`Flight`**: Represents individual flight trajectories.
- **`FlightCollection`**: Handles groups of flight trajectories.

### **Methods**
- **Resample and Interpolate**: Handle time-series data with methods like `.resample(freq)`.

You can find the full documentation [here](https://flightpandas.readthedocs.io/en/latest/index.html).

## **Dependencies**

- **Pandas**
- **GeoPandas**
- **Shapely**

Install dependencies using:
```bash
pip install pandas geopandas shapely
```


## **Contributing**

Contributions are welcome! To get started:

1. Fork the repository.
2. Create a new branch for your feature/bugfix:
   ```bash
   git checkout -b feature-name
   ```
3. Commit your changes and submit a pull request.


## **License**

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


## **Acknowledgments**

Special thanks to the contributors and the open-source community for making this library possible.


## **Contact**

For questions or support, please open an issue on the [GitHub repository](https://github.com/your_username/flightpandas).
