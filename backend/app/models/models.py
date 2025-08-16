# precipapi/core/models.py
"""Standardized models for multi-source precipitation data."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator

from app.integrations.usgs.models import (
    PrecipitationMeasurement as USGSPrecipitationMeasurement,
)
from app.integrations.usgs.models import (
    StreamflowMeasurement as USGSStreamflowMeasurement,
)
from app.integrations.usgs.models import USGSStationSummary


class ParameterType(str, Enum):
    """Standardized parameter types across all data sources."""

    PRECIPITATION = "precipitation"
    STREAMFLOW = "streamflow"
    TEMPERATURE_WATER = "temperature_water"
    TEMPERATURE_AIR = "temperature_air"
    GAGE_HEIGHT = "gage_height"
    HUMIDITY = "humidity"
    WIND_SPEED = "wind_speed"
    PRESSURE = "pressure"


class TimeInterval(str, Enum):
    """Standardized time intervals."""

    FIFTEEN_MINS = "15mins"
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


class DataSource(str, Enum):
    """Supported data sources."""

    USGS = "usgs"
    NOAA = "noaa"
    NWIS = "nwis"
    MESONET = "mesonet"


class Station(BaseModel):
    """Standardized station information across all data sources."""

    # Universal identifiers
    station_id: str  # Source-prefixed ID (e.g., "usgs:01646500")
    source: DataSource
    vendor_id: str  # Original source ID

    # Basic info
    name: str
    site_type: str

    # Location
    latitude: float
    longitude: float
    elevation_ft: float | None = None

    # Administrative
    state: str | None = None
    county: str | None = None

    # Capabilities
    available_parameters: list[ParameterType] = []

    # Search context
    distance_miles: float | None = None

    # Source-specific metadata
    metadata: dict[str, Any] = {}

    @classmethod
    def from_usgs_station(cls, usgs_station: "USGSStationSummary") -> "Station":
        """Convert USGS station to standardized format."""
        return cls(
            station_id=f"usgs:{usgs_station.site_no}",
            source=DataSource.USGS,
            vendor_id=usgs_station.site_no,
            name=usgs_station.site_name,
            site_type=usgs_station.site_type,
            latitude=usgs_station.latitude,
            longitude=usgs_station.longitude,
            elevation_ft=usgs_station.elevation_ft,
            state=usgs_station.state_cd,
            county=usgs_station.county_cd,
            available_parameters=cls._map_usgs_parameters(
                usgs_station.available_parameters
            ),
            distance_miles=usgs_station.distance_miles,
            metadata={
                "huc_cd": usgs_station.huc_cd,
                "usgs_site_type": usgs_station.site_type,
            },
        )

    @staticmethod
    def _map_usgs_parameters(usgs_params: list[str]) -> list[ParameterType]:
        """Map USGS parameter codes to standardized types."""
        mapping = {
            "00045": ParameterType.PRECIPITATION,
            "00046": ParameterType.PRECIPITATION,
            "00060": ParameterType.STREAMFLOW,
            "00065": ParameterType.GAGE_HEIGHT,
            "00010": ParameterType.TEMPERATURE_WATER,
            "00020": ParameterType.TEMPERATURE_AIR,
        }
        return [mapping[code] for code in usgs_params if code in mapping]


class Measurement(BaseModel):
    """Base class for all measurement types."""

    station_id: str
    source: DataSource
    vendor_id: str
    station_name: str
    parameter_type: ParameterType

    latitude: float
    longitude: float

    timestamp: datetime
    value: float | None
    unit: str

    quality_flags: list[str] = []
    metadata: dict[str, Any] = {}

    @field_validator("value", mode="before")
    @classmethod
    def parse_value(cls, v):
        """Parse value handling None and empty strings."""
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


class PrecipitationMeasurement(Measurement):
    """Standardized precipitation measurement."""

    parameter_type: ParameterType = Field(default=ParameterType.PRECIPITATION)

    @classmethod
    def from_usgs_measurement(
        cls, usgs_measurement: "USGSPrecipitationMeasurement"
    ) -> "PrecipitationMeasurement":
        """Convert USGS precipitation measurement to standardized format."""
        return cls(
            station_id=f"usgs:{usgs_measurement.site_no}",
            source=DataSource.USGS,
            vendor_id=usgs_measurement.site_no,
            station_name=usgs_measurement.site_name,
            latitude=usgs_measurement.latitude,
            longitude=usgs_measurement.longitude,
            timestamp=usgs_measurement.timestamp,
            value=usgs_measurement.value,
            unit=usgs_measurement.unit,
            quality_flags=usgs_measurement.qualifiers,
            metadata={"usgs_qualifiers": usgs_measurement.qualifiers},
        )


class StreamflowMeasurement(Measurement):
    """Standardized streamflow measurement."""

    parameter_type: ParameterType = Field(default=ParameterType.STREAMFLOW)

    @classmethod
    def from_usgs_measurement(
        cls, usgs_measurement: "USGSStreamflowMeasurement"
    ) -> "StreamflowMeasurement":
        """Convert USGS streamflow measurement to standardized format."""
        return cls(
            station_id=f"usgs:{usgs_measurement.site_no}",
            source=DataSource.USGS,
            vendor_id=usgs_measurement.site_no,
            station_name=usgs_measurement.site_name,
            latitude=usgs_measurement.latitude,
            longitude=usgs_measurement.longitude,
            timestamp=usgs_measurement.timestamp,
            value=usgs_measurement.value,
            unit=usgs_measurement.unit,
            quality_flags=usgs_measurement.qualifiers,
            metadata={"usgs_qualifiers": usgs_measurement.qualifiers},
        )


class TemperatureMeasurement(Measurement):
    """Standardized temperature measurement."""

    parameter_type: ParameterType = Field(default=ParameterType.TEMPERATURE_WATER)

    # Override to specify air vs water temperature
    def __init__(self, **data):
        if "parameter_type" not in data:
            # Default logic or you can determine from metadata
            data["parameter_type"] = ParameterType.TEMPERATURE_WATER
        super().__init__(**data)


# Search and response models
class StationSearchRequest(BaseModel):
    """Request parameters for station search."""

    # Location (one is required)
    latitude: float | None = None
    longitude: float | None = None
    address: str | None = None

    # Filters
    radius_miles: float = 25.0
    parameter_types: list[ParameterType] | None = None
    sources: list[DataSource] | None = None
    site_types: list[str] | None = None

    # Limits
    max_results: int = 100


class StationSearchResponse(BaseModel):
    """Response from station search."""

    stations: list[Station]
    total_count: int
    search_location: dict[str, Any]
    radius_miles: float
    errors_by_source: dict[str, str] = {}

    @property
    def count(self) -> int:
        """Alias for total_count for backward compatibility."""
        return self.total_count


class DataRequest(BaseModel):
    """Request for measurement data."""

    station_id: str
    parameter_type: ParameterType
    start_date: str  # ISO format
    end_date: str  # ISO format
    interval: TimeInterval = TimeInterval.DAY


class DataResponse(BaseModel):
    """Response containing measurement data."""

    station_id: str
    parameter_type: ParameterType
    measurements: list[Measurement]
    total_count: int
    date_range: dict[str, str]
    metadata: dict[str, Any] = {}

    @property
    def count(self) -> int:
        """Alias for total_count for backward compatibility."""
        return self.total_count
