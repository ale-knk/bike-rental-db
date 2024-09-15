# bike_rental_core/db/error_handler.py

from pymongo.errors import DuplicateKeyError, PyMongoError
from pydantic import ValidationError
from .base import BaseErrorHandler

class DuplicateTripError(Exception):
    """Raised when a trip with a duplicate key is inserted."""
    def __init__(self, message="Duplicate trip error"):
        self.message = message
        super().__init__(self.message)

class InvalidTripError(Exception):
    """Raised when a trip document fails validation."""
    def __init__(self, message="Invalid trip document error"):
        self.message = message
        super().__init__(self.message)

class TripsErrorHandler(BaseErrorHandler):
    def handle_error(self, error: Exception):
        if isinstance(error, DuplicateKeyError):
            raise DuplicateTripError(f"Duplicate trip error: {str(error)}")
        elif isinstance(error, ValidationError):
            raise InvalidTripError(f"Trip document validation error: {str(error)}")
        elif isinstance(error, PyMongoError):
            raise PyMongoError(f"MongoDB error: {str(error)}")
        else:
            super().handle_error(error)
