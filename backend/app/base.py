# precipapi/base.py
"""Base classes and interfaces for PrecipAPI connectors."""

import math
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Self

import requests.auth
from requests import Session

from app.models.exceptions import DataSourceError
from app.models.models import (
    ParameterType,
    PrecipitationMeasurement,
    Station,
    StreamflowMeasurement,
    TimeInterval,
)


class DataSourceConnector(ABC):
    """
    Abstract interface for precipitation data source connectors.
    Each connector wraps a specific API (USGS, NOAA, etc.) and implements
    standardized methods for station discovery and data retrieval.
    All methods should return data in normalized formats.
    """

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        """Return the canonical name for this data source."""
        pass

    @classmethod
    def init_from_request_context(cls, **kwargs: Any) -> Self:
        """Initialize connector from request context (optional override)."""
        raise NotImplementedError()

    def __init__(
        self, auth: requests.auth.AuthBase | None = None, session: Session | None = None
    ) -> None:
        """Initialize connector with optional authentication."""
        self.auth = auth
        self.session = session or Session()
        if auth:
            self.session.auth = auth

    # STATION DISCOVERY

    @abstractmethod
    def find_stations_by_coordinates(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float,
        parameter_types: list[ParameterType] | None = None,
    ) -> list[Station]:
        """Find stations within radius of coordinates."""
        pass

    def find_stations_by_address(
        self,
        address: str,
        radius_miles: float,
        parameter_types: list[ParameterType] | None = None,
    ) -> list[Station]:
        """Find stations within radius of address (default: not implemented)."""
        raise DataSourceError(f"{self.name()} does not support address-based search")

    @abstractmethod
    def get_station_info(self, station_id: str) -> Station:
        """Get detailed information about a specific station."""
        pass

    # DATA RETRIEVAL

    def get_precipitation_data(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        interval: TimeInterval,
    ) -> list[PrecipitationMeasurement]:
        """Get precipitation data (default: not supported)."""
        raise DataSourceError(
            f"{self.name()} does not support precipitation data retrieval"
        )

    def get_streamflow_data(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        interval: TimeInterval,
    ) -> list[StreamflowMeasurement]:
        """Get streamflow data (default: not supported)."""
        raise DataSourceError(
            f"{self.name()} does not support streamflow data retrieval"
        )

    def get_temperature_data(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        interval: TimeInterval,
    ) -> list[dict[str, Any]]:
        """Get temperature data (default: not supported)."""
        raise DataSourceError(
            f"{self.name()} does not support temperature data retrieval"
        )

    # UTILITY METHODS

    def _calculate_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate distance between two points in miles using Haversine formula."""
        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        # Radius of earth in miles
        r = 3956
        return c * r

    def _normalize_parameter_code(self, raw_code: str) -> ParameterType | None:
        """Convert source-specific parameter codes to standardized types."""
        # Override in subclasses with source-specific mappings
        return None

    def _validate_date_range(
        self, start_date: str, end_date: str
    ) -> tuple[datetime, datetime]:
        """Validate and parse date range."""
        try:
            start_dt = datetime.fromisoformat(start_date)
            end_dt = datetime.fromisoformat(end_date)
        except ValueError as e:
            raise DataSourceError(f"Invalid date format: {e}") from e

        if start_dt >= end_dt:
            raise DataSourceError("Start date must be before end date")

        return start_dt, end_dt

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        if hasattr(self.session, "close"):
            self.session.close()
