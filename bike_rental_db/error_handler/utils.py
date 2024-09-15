from typing import Callable, Any
from bike_rental_db.error_handler import BaseErrorHandler

def execute_with_error_handling(func: Callable, error_handler: BaseErrorHandler, *args, **kwargs) -> Any:
    try:
        return func(*args, **kwargs)
    except Exception as e:
        error_handler.handle_error(e)
