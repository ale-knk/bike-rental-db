from pymongo.errors import DuplicateKeyError, PyMongoError
from pydantic import ValidationError
from .base import BaseErrorHandler

class DuplicateStationError(Exception):
    """Raised when a station with a duplicate key is inserted."""
    def __init__(self, message="Duplicate station error"):
        self.message = message
        super().__init__(self.message)

class InvalidStationError(Exception):
    """Raised when a station document fails validation."""
    def __init__(self, message="Invalid station error"):
        self.message = message
        super().__init__(self.message)

class StationsErrorHandler(BaseErrorHandler):
    def handle_error(self, error: Exception):
        if isinstance(error, DuplicateKeyError):
            raise DuplicateStationError(f"Duplicate station error: {str(error)}")
        elif isinstance(error, ValidationError):
            raise InvalidStationError(f"Station document validation error: {str(error)}")
        elif isinstance(error, PyMongoError):
            raise PyMongoError(f"MongoDB error: {str(error)}")
        else:
            super().handle_error(error)