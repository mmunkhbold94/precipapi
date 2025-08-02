"""Pydantic models for USGS Water Services API responses."""

from datetime import datetime
from enum import Enum
from typing import Any, ClassVar

from pydantic import BaseModel, Field, validator


class USGSTimeZone(BaseModel):
    """Individual timezone information."""

    zone_offset: str = Field(alias="zoneOffset")
    zone_abbreviation: str = Field(alias="zoneAbbreviation")

    class Config:
        allow_population_by_field_name = True


class USGSTimeZoneInfo(BaseModel):
    """Time zone information from USGS API."""

    default_time_zone: USGSTimeZone = Field(alias="defaultTimeZone")
    daylight_savings_time_zone: USGSTimeZone = Field(alias="daylightSavingsTimeZone")
    site_uses_daylight_savings_time: bool = Field(alias="siteUsesDaylightSavingsTime")

    class Config:
        allow_population_by_field_name = True


class USGSGeogLocation(BaseModel):
    """Geographic location information."""

    srs: str
    latitude: float
    longitude: float


class USGSGeoLocation(BaseModel):
    """Geographic location wrapper."""

    geog_location: USGSGeogLocation = Field(alias="geogLocation")
    local_site_xy: list[Any] = Field(alias="localSiteXY")

    class Config:
        allow_population_by_field_name = True


class USGSSiteCode(BaseModel):
    """Site code information."""

    value: str
    network: str
    agency_code: str = Field(alias="agencyCode")

    class Config:
        allow_population_by_field_name = True


class USGSSiteProperty(BaseModel):
    """Site property information."""

    value: str
    name: str


class USGSSiteInfo(BaseModel):
    """USGS site information from sourceInfo."""

    site_name: str = Field(alias="siteName")
    site_code: list[USGSSiteCode] = Field(alias="siteCode")
    time_zone_info: USGSTimeZoneInfo = Field(alias="timeZoneInfo")
    geo_location: USGSGeoLocation = Field(alias="geoLocation")
    note: list[dict[str, Any]] = []
    site_type: list[dict[str, Any]] = Field(alias="siteType")
    site_property: list[USGSSiteProperty] = Field(alias="siteProperty")

    @property
    def site_no(self) -> str:
        """Get the primary site number."""
        if self.site_code:
            return self.site_code[0].value
        return ""

    @property
    def latitude(self) -> float:
        """Get latitude."""
        return self.geo_location.geog_location.latitude

    @property
    def longitude(self) -> float:
        """Get longitude."""
        return self.geo_location.geog_location.longitude

    class Config:
        allow_population_by_field_name = True


class USGSVariableCode(BaseModel):
    """Variable code information."""

    value: str
    network: str
    vocabulary: str
    variable_id: int = Field(alias="variableID")
    default: bool

    class Config:
        allow_population_by_field_name = True


class USGSUnit(BaseModel):
    """Unit information."""

    unit_code: str = Field(alias="unitCode")

    class Config:
        allow_population_by_field_name = True


class USGSVariable(BaseModel):
    """USGS parameter/variable information."""

    variable_code: list[USGSVariableCode] = Field(alias="variableCode")
    variable_name: str = Field(alias="variableName")
    variable_description: str = Field(alias="variableDescription")
    value_type: str = Field(alias="valueType")
    unit: USGSUnit
    options: dict[str, Any]
    note: list[dict[str, Any]] = []
    no_data_value: float | None = Field(None, alias="noDataValue")
    variable_property: list[dict[str, Any]] | None = Field(
        None, alias="variableProperty"
    )
    oid: str | None = None

    @property
    def parameter_code(self) -> str:
        """Get the primary parameter code."""
        if self.variable_code:
            return self.variable_code[0].value
        return ""

    @property
    def unit_abbreviation(self) -> str:
        """Get unit abbreviation."""
        return self.unit.unit_code

    class Config:
        allow_population_by_field_name = True


class USGSValue(BaseModel):
    """Individual measurement value from USGS."""

    value: str | None  # Can be numeric string or null
    qualifiers: list[str]
    date_time: datetime = Field(alias="dateTime")

    @validator("date_time", pre=True)
    def parse_datetime(cls, v):
        if isinstance(v, str):
            # Handle USGS datetime format
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v

    class Config:
        allow_population_by_field_name = True


class USGSTimeSeries(BaseModel):
    """Time series data from USGS."""

    source_info: USGSSiteInfo = Field(alias="sourceInfo")
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


# Remove the old USGSSiteInfo class since we renamed USGSSite to USGSSiteInfo


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

    @validator("value", pre=True)
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

    @validator("value", pre=True)
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
    elevation_ft: float | None = None
    available_parameters: list[str] = []
    distance_miles: float | None = None  # Distance from search point


"""Enums and constants for USGS Water Services API."""


class USGSParameterCode(str, Enum):
    """USGS parameter codes for different measurements."""

    # Water quantity parameters
    STREAMFLOW = "00060"  # Discharge, cubic feet per second
    GAGE_HEIGHT = "00065"  # Gage height, feet

    # Precipitation parameters
    PRECIPITATION = "00045"  # Precipitation, total, inches
    PRECIPITATION_ACCUMULATED = "00046"  # Precipitation, accumulated, inches

    # Water quality parameters (common ones)
    TEMPERATURE_WATER = "00010"  # Temperature, water, degrees Celsius
    TEMPERATURE_AIR = "00020"  # Temperature, air, degrees Celsius
    PH = "00400"  # pH, standard units
    DISSOLVED_OXYGEN = "00300"  # Dissolved oxygen, mg/L
    TURBIDITY = "63680"  # Turbidity, formazin nephelometric units (FNU)

    # Additional flow parameters
    VELOCITY = "00055"  # Stream velocity, feet per second
    RESERVOIR_STORAGE = "00054"  # Reservoir storage, acre-feet

    @classmethod
    def get_description(cls, code: str) -> str:
        """Get human-readable description for a parameter code."""
        descriptions = {
            cls.STREAMFLOW: "Streamflow (Discharge, cubic feet per second)",
            cls.GAGE_HEIGHT: "Gage Height (feet)",
            cls.PRECIPITATION: "Precipitation (total, inches)",
            cls.PRECIPITATION_ACCUMULATED: "Precipitation (accumulated, inches)",
            cls.TEMPERATURE_WATER: "Water Temperature (degrees Celsius)",
            cls.TEMPERATURE_AIR: "Air Temperature (degrees Celsius)",
            cls.PH: "pH (standard units)",
            cls.DISSOLVED_OXYGEN: "Dissolved Oxygen (mg/L)",
            cls.TURBIDITY: "Turbidity (FNU)",
            cls.VELOCITY: "Stream Velocity (feet per second)",
            cls.RESERVOIR_STORAGE: "Reservoir Storage (acre-feet)",
        }
        return descriptions.get(code, f"Unknown parameter: {code}")


class USGSTimePeriod(str, Enum):
    """USGS time period codes (ISO 8601 duration format)."""

    # Hours
    LAST_HOUR = "PT1H"
    LAST_6_HOURS = "PT6H"
    LAST_12_HOURS = "PT12H"

    # Days
    LAST_DAY = "P1D"
    LAST_2_DAYS = "P2D"
    LAST_3_DAYS = "P3D"
    LAST_WEEK = "P7D"
    LAST_14_DAYS = "P14D"

    # Months
    LAST_MONTH = "P1M"
    LAST_3_MONTHS = "P3M"
    LAST_6_MONTHS = "P6M"

    # Years
    LAST_YEAR = "P1Y"
    LAST_2_YEARS = "P2Y"

    @classmethod
    def get_description(cls, period: str) -> str:
        """Get human-readable description for a time period."""
        descriptions = {
            cls.LAST_HOUR: "Last hour",
            cls.LAST_6_HOURS: "Last 6 hours",
            cls.LAST_12_HOURS: "Last 12 hours",
            cls.LAST_DAY: "Last day",
            cls.LAST_2_DAYS: "Last 2 days",
            cls.LAST_3_DAYS: "Last 3 days",
            cls.LAST_WEEK: "Last week",
            cls.LAST_14_DAYS: "Last 14 days",
            cls.LAST_MONTH: "Last month",
            cls.LAST_3_MONTHS: "Last 3 months",
            cls.LAST_6_MONTHS: "Last 6 months",
            cls.LAST_YEAR: "Last year",
            cls.LAST_2_YEARS: "Last 2 years",
        }
        return descriptions.get(period, f"Custom period: {period}")


class USGSSiteType(str, Enum):
    """USGS site type codes."""

    STREAM = "ST"  # Stream
    LAKE = "LK"  # Lake, Reservoir, Impoundment
    WELL = "GW"  # Well
    SPRING = "SP"  # Spring
    ESTUARY = "ES"  # Estuary
    OCEAN = "OC"  # Ocean
    PRECIPITATION = "PR"  # Precipitation
    LAND = "LA"  # Land
    AGGREGATE_GROUNDWATER_USE = "AG"  # Aggregate groundwater use
    AGGREGATE_SURFACE_WATER_USE = "AS"  # Aggregate surface-water-use
    ATMOSPHERIC = "AT"  # Atmospheric

    @classmethod
    def get_description(cls, site_type: str) -> str:
        """Get human-readable description for a site type."""
        descriptions = {
            cls.STREAM: "Stream",
            cls.LAKE: "Lake, Reservoir, Impoundment",
            cls.WELL: "Well",
            cls.SPRING: "Spring",
            cls.ESTUARY: "Estuary",
            cls.OCEAN: "Ocean",
            cls.PRECIPITATION: "Precipitation",
            cls.LAND: "Land",
            cls.AGGREGATE_GROUNDWATER_USE: "Aggregate groundwater use",
            cls.AGGREGATE_SURFACE_WATER_USE: "Aggregate surface-water-use",
            cls.ATMOSPHERIC: "Atmospheric",
        }
        return descriptions.get(site_type, f"Unknown site type: {site_type}")


class USGSSiteStatus(str, Enum):
    """USGS site status codes."""

    ACTIVE = "active"  # Currently collecting data
    INACTIVE = "inactive"  # Not currently collecting data
    ALL = "all"  # Both active and inactive


class USGSAgency(str, Enum):
    """USGS agency codes."""

    USGS = "USGS"  # U.S. Geological Survey
    EPA = "EPA"  # Environmental Protection Agency
    CORPS = "USACE"  # U.S. Army Corps of Engineers


# Commonly used parameter groups
class USGSParameterGroups:
    """Commonly used parameter code groups."""

    FLOW_PARAMETERS: ClassVar = [
        USGSParameterCode.STREAMFLOW,
        USGSParameterCode.GAGE_HEIGHT,
        USGSParameterCode.VELOCITY,
    ]

    PRECIPITATION_PARAMETERS: ClassVar = [
        USGSParameterCode.PRECIPITATION,
        USGSParameterCode.PRECIPITATION_ACCUMULATED,
    ]

    TEMPERATURE_PARAMETERS: ClassVar = [
        USGSParameterCode.TEMPERATURE_WATER,
        USGSParameterCode.TEMPERATURE_AIR,
    ]

    WATER_QUALITY_PARAMETERS: ClassVar = [
        USGSParameterCode.TEMPERATURE_WATER,
        USGSParameterCode.PH,
        USGSParameterCode.DISSOLVED_OXYGEN,
        USGSParameterCode.TURBIDITY,
    ]


# Default values
class USGSDefaults:
    """Default values for USGS API calls."""

    DEFAULT_TIMEOUT = 30  # seconds
    DEFAULT_PERIOD = USGSTimePeriod.LAST_WEEK
    DEFAULT_SITE_STATUS = USGSSiteStatus.ACTIVE
    DEFAULT_RADIUS_MILES = 25.0
    MAX_SITES_PER_REQUEST = 100  # USGS API limitation
