"""USGS Water Services API client."""

import math
from datetime import datetime
from typing import Any

import httpx

from .models import (
    PrecipitationMeasurement,
    StreamflowMeasurement,
    USGSDefaults,
    USGSInstantaneousValuesResponse,
    USGSParameterCode,
)


class USGSException(Exception):
    """Base exception for USGS API errors."""

    pass


class USGSTimeoutError(USGSException):
    """Exception raised for timeout errors."""

    pass


class USGSNotFoundError(USGSException):
    """Exception raised when a resource is not found."""

    pass


class USGSServerError(USGSException):
    """Exception raised for server errors."""

    pass


class USGSClient:
    """Client for interacting with the USGS Water Services API."""

    BASE_URL = "https://waterservices.usgs.gov/nwis"

    def __init__(self, timeout: int = USGSDefaults.DEFAULT_TIMEOUT):
        self.timeout = timeout
        self._client = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        if self._client:
            await self._client.aclose()

    async def _request_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """Make a JSON request to the USGS API."""
        if params is None:
            params = {}
        if timeout is None:
            timeout = self.timeout

        if self._client is None:
            self._client = httpx.AsyncClient(timeout=timeout)

        url = f"{self.BASE_URL}{path}"

        try:
            response = await self._client.get(url, params=params)
        except httpx.TimeoutException:
            raise USGSTimeoutError(f"Timeout {timeout}s: {url}") from None

        if response.status_code == 404:
            raise USGSNotFoundError(f"Resource not found: {url}")
        elif response.status_code >= 500:
            raise USGSServerError(f"Server error {response.status_code}: {url}")
        elif response.status_code >= 400:
            raise USGSException(f"Client error {response.status_code}: {url}")

        try:
            return response.json()
        except Exception as e:
            raise USGSException(f"Invalid JSON response: {url}") from e

    # Data Retrieval Methods
    async def get_instantaneous_values(
        self,
        site_codes: list[str],
        parameter_codes: list[str],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        period: str | None = None,
    ) -> USGSInstantaneousValuesResponse:
        """Get instantaneous values for specified sites and parameters."""
        params = {
            "format": "json",
            "sites": ",".join(site_codes),
            "parameterCd": ",".join(parameter_codes),
        }

        if period:
            params["period"] = period
        elif start_date and end_date:
            params["startDT"] = start_date.strftime("%Y-%m-%dT%H:%M")
            params["endDT"] = end_date.strftime("%Y-%m-%dT%H:%M")
        else:
            # Default to last 7 dats if no time specified
            params["period"] = str(USGSDefaults.DEFAULT_PERIOD)

        response_data = await self._request_json("/iv/", params=params)
        return USGSInstantaneousValuesResponse(**response_data)

    async def get_precipitation_data(
        self,
        site_codes: list[str],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        period: str = USGSDefaults.DEFAULT_PERIOD,
    ) -> list[PrecipitationMeasurement]:
        """Get instantaneous precipitation data for specified sites."""
        response = await self.get_instantaneous_values(
            site_codes=site_codes,
            parameter_codes=[
                USGSParameterCode.PRECIPITATION
            ],  # Precipitation parameter code
            start_date=start_date,
            end_date=end_date,
            period=period,
        )
        measurements = []
        for time_series in response.time_series:
            site_info = time_series.source_info
            variable_info = time_series.variable

            unit = variable_info.unit_abbreviation

            for value_group in time_series.values:
                if "value" in value_group:
                    for value_data in value_group["value"]:
                        measurement = PrecipitationMeasurement(
                            site_no=site_info.site_no,
                            site_name=site_info.site_name,
                            latitude=site_info.latitude,
                            longitude=site_info.longitude,
                            value=value_data.get("value"),
                            unit=unit,
                            timestamp=value_data.get("dateTime"),
                            qualifiers=value_data.get("qualifiers", []),
                        )
                        measurements.append(measurement)
        return measurements

    async def get_streamflow_data(
        self,
        site_codes: list[str],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        period: str = USGSDefaults.DEFAULT_PERIOD,
    ) -> list[StreamflowMeasurement]:
        """Get instantaneous streamflow data for specified sites."""
        response = await self.get_instantaneous_values(
            site_codes=site_codes,
            parameter_codes=[USGSParameterCode.STREAMFLOW],  # Streamflow parameter code
            start_date=start_date,
            end_date=end_date,
            period=period,
        )
        measurements = []
        for time_series in response.time_series:
            site_info = time_series.source_info
            variable_info = time_series.variable

            unit = variable_info.unit_abbreviation

            for value_group in time_series.values:
                if "value" in value_group:
                    for value_data in value_group["value"]:
                        measurement = StreamflowMeasurement(
                            site_no=site_info.site_no,
                            site_name=site_info.site_name,
                            latitude=site_info.latitude,
                            longitude=site_info.longitude,
                            value=value_data.get("value"),
                            unit=unit,
                            timestamp=value_data.get("dateTime"),
                            qualifiers=value_data.get("qualifiers", []),
                        )
                        measurements.append(measurement)
        return measurements

    def _calculate_bounding_box(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float,
    ) -> str:
        """
        Calculate a bounding box for a given latitude, longitude, and radius.
        Currently is a rough approximation using a square bounding box.
        Need to improve this by using libs like geopy.
        """
        lat_delta = radius_miles / 69.0  # Roughly 69 miles per degree latitude
        lon_delta = radius_miles / (
            69.0 * abs(latitude / 90.0) if latitude != 0 else 69.0
        )  # Adjust for longitude based on latitude

        # Bounding box format: west, south, east, north
        west = longitude - lon_delta
        south = latitude - lat_delta
        east = longitude + lon_delta
        north = latitude + lat_delta

        return f"{west},{south},{east},{north}"

    def _calculate_distance(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
    ) -> float:
        """Calculate the distance between two points on the Earth's surface."""
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        r = 3956

        return c * r
