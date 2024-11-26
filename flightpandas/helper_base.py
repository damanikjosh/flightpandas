"""
helper_base.py

This module defines the `HelperBase` class, which serves as a foundational base for operations on 
`Flight` and `FlightCollection` objects. The `HelperBase` class is compatible with pandas-style 
method chaining through the `pipe` method, enabling flexible and reusable transformations.

Classes:
--------
HelperBase:
    A base class for creating operations on `Flight` and `FlightCollection` objects, 
    supporting sequential pipelines for data processing and transformations.

Examples:
---------
# Create a HelperBase instance
helper = HelperBase(flight)

# Add transformations to the pipeline
helper = helper.pipe(transformation_func_1).pipe(transformation_func_2)

# Evaluate the pipeline
result = helper.eval()
"""
from flightpandas.flight import Flight
from flightpandas.flight_collection import FlightCollection

class HelperBase:
    """
    A base class for operations on `Flight` and `FlightCollection` objects, enabling method 
    chaining and reusable transformation pipelines.

    Attributes:
    -----------
    data : Flight | FlightCollection
        The data object (`Flight` or `FlightCollection`) to operate on.
    pipes : list
        A list of applied operations for tracking the transformation pipeline.

    Methods:
    --------
    __init__(obj, *args, **kwargs):
        Initializes the HelperBase with a data object and tracks the transformation pipeline.
    eval(*args, **kwargs):
        Evaluates all the transformations in the pipeline sequentially and applies them to the data object.
    pipe(func, *args, **kwargs):
        Adds a transformation function to the pipeline and returns the updated HelperBase.
    _eval_flight(obj, *args, **kwargs):
        Processes a single `Flight` object and applies custom transformations.
    _eval_flight_collection(obj, *args, **kwargs):
        Processes a `FlightCollection` object and applies custom transformations.
    """

    def __init__(self, obj, *args, **kwargs):
        """
        Initializes the HelperBase with a `Flight` or `FlightCollection` object.

        Parameters:
        -----------
        obj : Flight | FlightCollection
            The input data object to be processed.
        *args : tuple
            Additional positional arguments for initialization.
        **kwargs : dict
            Additional keyword arguments for initialization.

        Raises:
        -------
        ValueError:
            If the input object is not a `Flight` or `FlightCollection` instance.
        """
        if isinstance(obj, HelperBase):
            self.pipes = obj.pipes
            self.data = obj.data
        else:
            if not isinstance(obj, (Flight, FlightCollection)):
                raise ValueError("Input must be of type `Flight` or `FlightCollection`. "
                                 f"Got {type(obj).__name__} instead.")
            self.data = obj
            self.pipes = []
        self.pipes = self.pipes + [self]

    def eval(self, *args, **kwargs) -> Flight | FlightCollection:
        """
        Evaluates all the transformations in the pipeline sequentially.

        Each transformation in `self.pipes` is applied to the data object. Depending on the data type,
        the transformation is delegated to `_eval_flight` or `_eval_flight_collection`.

        Parameters:
        -----------
        *args : tuple
            Additional positional arguments for pipeline functions.
        **kwargs : dict
            Additional keyword arguments for pipeline functions.

        Returns:
        --------
        Flight | FlightCollection:
            The result after applying all transformations in the pipeline.

        Raises:
        -------
        TypeError:
            If a pipe in the pipeline is not an instance of `HelperBase`.
        ValueError:
            If the data object has an unexpected type during evaluation.
        """
        data = self.data

        for pipe in self.pipes:
            # Validate the current pipe
            if not isinstance(pipe, HelperBase):
                raise TypeError(f"Invalid pipe detected in the pipeline: {pipe}. "
                                "All pipes must inherit from `HelperBase`.")
            
            # Apply the transformation based on the type of `data`
            if isinstance(data, Flight):
                data = pipe._eval_flight(data, *args, **kwargs)
            elif isinstance(data, FlightCollection):
                data = pipe._eval_flight_collection(data, *args, **kwargs)
            else:
                raise ValueError("Unexpected data type encountered during evaluation: "
                                f"{type(data).__name__}.")
            

        return data


    def pipe(self, func: callable, *args, **kwargs) -> "HelperBase":
        """
        Adds a function to the transformation pipeline.

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
            The updated `HelperBase` object with the new transformation function added.
        """
        return func(self, *args, **kwargs)

    def _eval_flight(self, obj: Flight, *args, **kwargs) -> Flight | FlightCollection:
        """
        Processes a single `Flight` object and applies transformations.

        Parameters:
        -----------
        obj : Flight
            The `Flight` object to process.
        *args : tuple
            Additional positional arguments for the transformation.
        **kwargs : dict
            Additional keyword arguments for the transformation.

        Returns:
        --------
        Flight | FlightCollection:
            The processed `Flight` object or resulting collection.
        """
        return obj

    def _eval_flight_collection(self, obj: FlightCollection, *args, **kwargs) -> Flight | FlightCollection:
        """
        Processes a `FlightCollection` object and applies transformations.

        Parameters:
        -----------
        obj : FlightCollection
            The `FlightCollection` object to process.
        *args : tuple
            Additional positional arguments for the transformation.
        **kwargs : dict
            Additional keyword arguments for the transformation.

        Returns:
        --------
        Flight | FlightCollection:
            The processed `FlightCollection` object or resulting collection.
        """
        return obj
