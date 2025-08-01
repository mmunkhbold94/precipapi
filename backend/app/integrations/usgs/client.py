"""USGS Water Services API client."""
from typing import Any
from datetime import datetime
import httpx


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

    def __init__(self, timeout: int = 30):
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

    async def find_stations_by_location(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float = 10.0,
        parameter_codes: list[str] | None = None,
        site_types: list[str] | None = None,
    ) -> dict[str, Any]:
        """Find USGS stations within a radius of a lat/lon point."""
        params = {
            "format": "json",
            "bBox": self._calculate_bounding_box(latitude, longitude, radius_miles),
            "siteStatus": "all",
        }

        if parameter_codes:
            params["parameterCd"] = ",".join(parameter_codes)

        if site_types:
            params["siteType"] = ",".join(site_types)

        return await self._request_json("/sites", params=params)

    async def find_precipitation_stations(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float = 10.0,
    ) -> dict[str, Any]:
        """Find precipitation monitoring stations near a location."""
        return await self.find_stations_by_location(
            latitude=latitude,
            longitude=longitude,
            radius_miles=radius_miles,
            parameter_codes=["00045"],  # Precipitation parameter code
        )

    async def find_streamflow_stations(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float = 10.0,
    ) -> dict[str, Any]:
        """Find streamflow monitoring stations near a location."""
        return await self.find_stations_by_location(
            latitude=latitude,
            longitude=longitude,
            radius_miles=radius_miles,
            parameter_codes=["00060"],  # Streamflow parameter code
            site_types=["ST"] # Stream sites only
        )

    # Data Retrieval Methods
    async def get_instantaneous_values(
        self,
        site_codes: list[str],
        parameter_codes: list[str],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        period: str | None = None,
    ) -> dict[str, Any]:
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
            params["period"] = "P7D"

        return await self._request_json("/iv/", params=params)

    async def get_precipitation_data(
        self,
        site_codes: list[str],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        period: str = "P7D",
    ) -> dict[str, Any]:
        """Get instantaneous precipitation data for specified sites."""
        return await self.get_instantaneous_values(
            site_codes=site_codes,
            parameter_codes=["00045"],  # Precipitation parameter code
            start_date=start_date,
            end_date=end_date,
            period=period,
        )

    async def get_streamflow_data(
        self,
        site_codes: list[str],
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        period: str = "P7D",
    ) -> dict[str, Any]:
        """Get instantaneous streamflow data for specified sites."""
        return await self.get_instantaneous_values(
            site_codes=site_codes,
            parameter_codes=["00060"],  # Streamflow parameter code
            start_date=start_date,
            end_date=end_date,
            period=period,
        )

    async def get_site_info(
        self,
        site_codes: list[str],
    ) -> dict[str, Any]:
        """Get detailed information for specified sites."""
        params = {
            "format": "json",
            "sites": ",".join(site_codes),
            "siteOutput": "expanded",
        }
        return await self._request_json("/site/", params=params)

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
        lon_delta = radius_miles / (69.0 * abs(latitude / 90.0) if latitude != 0 else 69.0)  # Adjust for longitude based on latitude

        # Bounding box format: west, south, east, north
        west = longitude - lon_delta
        south = latitude - lat_delta
        east = longitude + lon_delta
        north = latitude + lat_delta

        return f"{west},{south},{east},{north}"
