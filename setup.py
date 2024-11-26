from setuptools import setup, find_packages

setup(
    name="flightpandas",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "geopandas",
        "shapely",
        "pyarrow",
    ],
    extras_require={
        "dev": ["pytest"]
    },
    test_suite="tests",
)
