from flightpandas.flight import Flight
from flightpandas.flight_collection import FlightCollection

class HelperBase:
    """
    Base class for operations on Flight and FlightCollection objects, compatible
    with the pandas `pipe` method for chaining operations.

    Attributes:
        data (Flight | FlightCollection): The data object to operate on.
        pipes (list): A list of applied operations for tracking the pipeline.
    """

    def __init__(self, obj, *args, **kwargs):
        """
        Initializes the HelperBase with a Flight or FlightCollection object.
        
        Args:
            obj (Flight | FlightCollection): The input data object.
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
        Applies all the transformations in the pipeline sequentially.

        Each pipe in `self.pipes` is applied to the current data object.
        The transformations are delegated to either `_eval_flight` or
        `_eval_flight_collection` depending on the type of the current data.

        Args:
            *args: Positional arguments for the pipeline functions.
            **kwargs: Keyword arguments for the pipeline functions.

        Returns:
            FlightCollection: The final result after applying all transformations.
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
        Applies a function in the pipeline and returns the resulting HelperBase.

        Args:
            func (callable): The function to apply.
        
        Returns:
            HelperBase: The resulting object after applying the function.
        """
        return func(self, *args, **kwargs)

    def _eval_flight(self, obj: Flight, *args, **kwargs) -> Flight | FlightCollection:
        """
        Processes a single Flight object.

        Args:
            flight (Flight): The Flight object to process.

        Returns:
            FlightCollection: The resulting FlightCollection.
        """
        return obj

    def _eval_flight_collection(self, obj: FlightCollection, *args, **kwargs) -> Flight | FlightCollection:
        """
        Processes a FlightCollection object.

        Args:
            fc (FlightCollection): The FlightCollection object to process.

        Returns:
            FlightCollection: The processed FlightCollection.
        """
        return obj
