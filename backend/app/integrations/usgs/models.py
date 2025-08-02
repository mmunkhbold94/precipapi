"""Pydantic models for USGS Water Services API responses."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class USGSTimeZoneInfo(BaseModel):
    """Time zone information from USGS API."""

    zone_offset: str = Field(alias="zoneOffset")
    zone_abbreviation: str = Field(alias="zoneAbbreviation")


class USGSGeoLocation(BaseModel):
    """Geographic location information for USGS sites."""

    latitude: float
    longitude: float
    srs: str  # Spatial reference system
    accuracy: float | None = None


class USGSSiteProperty(BaseModel):
    """Site property information."""

    value: str
    name: str


class USGSSite(BaseModel):
    """USGS monitoring site information."""

    agency_cd: str = Field(alias="agencyCD")
    site_no: str = Field(alias="siteCode")
    site_name: str = Field(alias="siteName")
    site_type_cd: str = Field(alias="siteTypeCD")
    huc_cd: str | None = Field(None, alias="hucCD")
    county_cd: str | None = Field(None, alias="countyCD")
    state_cd: str | None = Field(None, alias="stateCD")
    geo_location: USGSGeoLocation = Field(alias="geoLocation")
    elevation_value: float | None = Field(None, alias="elevation_va")
    elevation_accuracy: float | None = Field(None, alias="elevation_ac")
    elevation_datum: float | None = Field(None, alias="elevation_mc")
    altitude_value: float | None = Field(None, alias="altitudeVa")
    altitude_accuracy: float | None = Field(None, alias="altitudeAc")
    altitude_datum: str | None = Field(None, alias="altitudeMc")
    hydrologic_unit_cd: str | None = Field(None, alias="hydrologicUnitCD")
    basin_cd: str | None = Field(None, alias="basinCD")
    topo_cd: str | None = Field(None, alias="topoCD")
    instruments: list[dict[str, Any]] | None = None
    site_property: list[USGSSiteProperty] | None = Field(None, alias="siteProperty")

    class Config:
        allow_population_by_field_name = True


class USGSVariable(BaseModel):
    """USGS parameter/variable information."""

    variable_code: str = Field(alias="variableCode")
    variable_name: str = Field(alias="variableName")
    variable_description: str = Field(alias="variableDescription")
    value_type: str = Field(alias="valueType")
    unit: dict[str, Any]
    options: dict[str, Any]
    note: list[dict[str, Any]] | None = None
    no_data_value: float = Field(alias="noDataValue")

    class Config:
        allow_population_by_field_name = True


class USGSValue(BaseModel):
    """Individual measurement value from USGS."""

    value: str | None  # Can be numeric string or null
    qualifiers: list[str]
    date_time: datetime = Field(alias="dateTime")

    @field_validator("date_time", mode="before")
    @classmethod
    def parse_datetime(cls, v):
        if isinstance(v, str):
            # Handle USGS datetime format
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v

    class Config:
        allow_population_by_field_name = True


class USGSTimeSeries(BaseModel):
    """Time series data from USGS."""

    source_info: USGSSite = Field(alias="sourceInfo")
    variable: USGSVariable
    values: list[dict[str, Any]]  # Contains method info and value list
    name: str

    class Config:
        allow_population_by_field_name = True


class USGSTimeSeriesResponse(BaseModel):
    """Response structure for USGS time series data endpoints."""

    name: str
    declared_type: str = Field(alias="declaredType")
    scope: str
    nil: bool
    global_scope: bool = Field(alias="globalScope")
    type_substituted: bool = Field(alias="typeSubstituted")
    value: dict[str, Any]  # Contains queryInfo and timeSeries

    class Config:
        allow_population_by_field_name = True


class USGSQueryInfo(BaseModel):
    """Query information metadata."""

    query_url: str = Field(alias="queryURL")
    criteria: dict[str, Any]
    note: list[dict[str, Any]] | None = None

    class Config:
        allow_population_by_field_name = True


class USGSInstantaneousValuesResponse(BaseModel):
    """Complete response structure for instantaneous values endpoint."""

    name: str
    declared_type: str = Field(alias="declaredType")
    scope: str
    nil: bool
    global_scope: bool = Field(alias="globalScope")
    type_substituted: bool = Field(alias="typeSubstituted")
    value: dict[str, Any]  # Contains queryInfo and timeSeries

    @property
    def query_info(self) -> USGSQueryInfo | None:
        """Extract query info from the response."""
        if "queryInfo" in self.value:
            return USGSQueryInfo(**self.value["queryInfo"])
        return None

    @property
    def time_series(self) -> list[USGSTimeSeries]:
        """Extract time series data from the response."""
        if "timeSeries" in self.value:
            return [USGSTimeSeries(**ts) for ts in self.value["timeSeries"]]
        return []

    class Config:
        allow_population_by_field_name = True


class USGSSiteInfo(BaseModel):
    """Site information response structure."""

    name: str
    declared_type: str = Field(alias="declaredType")
    scope: str
    nil: bool
    global_scope: bool = Field(alias="globalScope")
    type_substituted: bool = Field(alias="typeSubstituted")
    value: dict[str, Any]  # Contains queryInfo and site list

    @property
    def query_info(self) -> USGSQueryInfo | None:
        """Extract query info from the response."""
        if "queryInfo" in self.value:
            return USGSQueryInfo(**self.value["queryInfo"])
        return None

    @property
    def sites(self) -> list[USGSSite]:
        """Extract sites from the response."""
        if "site" in self.value:
            return [USGSSite(**site) for site in self.value["site"]]
        return []

    class Config:
        allow_population_by_field_name = True


# Convenience models for specific data types
class PrecipitationMeasurement(BaseModel):
    """Processed precipitation measurement."""

    site_no: str
    site_name: str
    latitude: float
    longitude: float
    value: float | None  # Precipitation in inches
    unit: str
    timestamp: datetime
    qualifiers: list[str]

    @field_validator("value", mode="before")
    @classmethod
    def parse_value(cls, v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


class StreamflowMeasurement(BaseModel):
    """Processed streamflow measurement."""

    site_no: str
    site_name: str
    latitude: float
    longitude: float
    value: float | None  # Streamflow in cubic feet per second
    unit: str
    timestamp: datetime
    qualifiers: list[str]

    @field_validator("value", mode="before")
    @classmethod
    def parse_value(cls, v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


class USGSStationSummary(BaseModel):
    """Simplified station information for API responses."""

    site_no: str
    site_name: str
    site_type: str
    latitude: float
    longitude: float
    state_cd: str | None = None
    county_cd: str | None = None
    huc_cd: str | None = None
    elevation_ft: str | None = None
    available_parameters: list[str] = []
    distance_miles: str | None = None  # Distance from search point
