from pymongo.errors import DuplicateKeyError, PyMongoError
from pydantic import ValidationError
from .base import BaseErrorHandler

class DuplicateStatusError(Exception):
    """Raised when a Status with a duplicate key is inserted."""
    def __init__(self, message="Duplicate Status error"):
        self.message = message
        super().__init__(self.message)

class InvalidStatusError(Exception):
    """Raised when a Status document fails validation."""
    def __init__(self, message="Invalid Status error"):
        self.message = message
        super().__init__(self.message)

class StationStatusErrorHandler(BaseErrorHandler):
    def handle_error(self, error: Exception):
        if isinstance(error, DuplicateKeyError):
            raise DuplicateStatusError(f"Duplicate Status error: {str(error)}")
        elif isinstance(error, ValidationError):
            raise InvalidStatusError(f"Status document validation error: {str(error)}")
        elif isinstance(error, PyMongoError):
            raise PyMongoError(f"MongoDB error: {str(error)}")
        else:
            super().handle_error(error)