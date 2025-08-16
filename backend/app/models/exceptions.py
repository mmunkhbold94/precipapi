"""Custom exceptions for PrecipAPI."""

from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


class PrecipAPIError(Exception):
    """Base exception for PrecipAPI errors."""

    pass


class StationNotFound(PrecipAPIError):
    """Raised when a requested station cannot be found."""

    pass


class DataSourceError(PrecipAPIError):
    """Raised when a data source encounters an error."""

    pass


class ValidationError(PrecipAPIError):
    """Raised when input validation fails."""

    pass


class AuthenticationError(PrecipAPIError):
    """Raised when authentication fails for a data source."""

    pass


class RateLimitError(DataSourceError):
    """Raised when API rate limits are exceeded."""

    pass


def only_precipapi_exceptions(func: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator to ensure only PrecipAPI exceptions are raised.
    Converts unexpected exceptions to DataSourceError to maintain
    clean API boundaries.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except PrecipAPIError:
            # Re-raise our own exceptions
            raise
        except Exception as e:
            # Convert unexpected exceptions
            raise DataSourceError(f"Unexpected error: {e}") from e

    return wrapper
