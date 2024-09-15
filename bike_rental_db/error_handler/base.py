# bike_rental_core/db/error_handler.py
from pymongo.errors import DuplicateKeyError, PyMongoError
from pydantic import ValidationError

class DuplicateDocumentError(Exception):
    """Raised when a document with a duplicate key is inserted."""
    def __init__(self, message="Duplicate document error"):
        self.message = message
        super().__init__(self.message)

class InvalidDocumentError(Exception):
    """Raised when a document fails validation."""
    def __init__(self, message="Invalid document error"):
        self.message = message
        super().__init__(self.message)

class BaseErrorHandler:
    def handle_error(self, error: Exception):
        if isinstance(error, DuplicateKeyError):
            raise DuplicateDocumentError(f"Duplicate document error: {str(error)}")
        elif isinstance(error, ValidationError):
            raise InvalidDocumentError(f"Document validation error: {str(error)}")
        elif isinstance(error, PyMongoError):
            raise PyMongoError(f"MongoDB error: {str(error)}")
        else:
            raise error
