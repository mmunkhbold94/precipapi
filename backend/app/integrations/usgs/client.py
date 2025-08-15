"""USGS Water Services API client."""

import asyncio
import contextlib
import math
from datetime import datetime
from functools import partial
from typing import Any

import httpx
from geopy.distance import geodesic
from geopy.geocoders import Nominatim

from .models import (
    PrecipitationMeasurement,
    StreamflowMeasurement,
    USGSDefaults,
    USGSInstantaneousValuesResponse,
    USGSParameterCode,
    USGSStationSummary,
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

    async def _request_text(
        self,
        endpoint: str,
        params: dict | None = None,
    ) -> str:
        """Make a text request to the USGS API."""
        if params is None:
            params = {}

        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)

        url = f"{self.BASE_URL}{endpoint}"

        try:
            response = await self._client.get(url, params=params)
        except httpx.TimeoutException:
            raise USGSTimeoutError(f"Timeout {self.timeout}s: {url}") from None

        if response.status_code == 404:
            raise USGSNotFoundError(f"Resource not found: {url}")
        elif response.status_code >= 500:
            raise USGSServerError(f"Server error {response.status_code}: {url}")
        elif response.status_code >= 400:
            raise USGSException(f"Client error {response.status_code}: {url}")

        return response.text

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

    async def get_sites_by_coordinates(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float = 50,
        site_type: str | None = None,
        has_data_type_cd: str | None = None,
    ) -> list[USGSStationSummary]:
        """Set sites based on geographic coordinates."""
        params = {
            "format": "rdb",  # Site Service API has RDB (tab-delimited) format
            "lat": str(latitude),
            "long": str(longitude),
            "radius": str(radius_miles),
            "siteOutput": "expanded",
        }
        if site_type:
            params["siteType"] = site_type
        if has_data_type_cd:
            params["hasDataTypeCd"] = has_data_type_cd

        response_text = await self._request_text("/site/", params=params)

        stations = self._parse_rdb_sites_response(response_text, latitude, longitude)

        return stations

    async def get_sites_by_address(
        self,
        address: str,
        radius_miles: float = 50,
        site_type: str | None = None,
        has_data_type_cd: str | None = None,
    ) -> tuple[dict, list[USGSStationSummary]]:
        """Get sites based on an address."""
        geolocator = Nominatim(user_agent="usgs_water_api")
        loop = asyncio.get_running_loop()
        location: Any = await loop.run_in_executor(
            None, partial(geolocator.geocode, address)
        )

        if not location:
            raise USGSException(f"Address not found: {address}")

        location_info = {
            "address": address,
            "resolved_address": location.address,
            "latitude": location.latitude,
            "longitude": location.longitude,
        }

        stations = await self.get_sites_by_coordinates(
            latitude=location.latitude,
            longitude=location.longitude,
            radius_miles=radius_miles,
            site_type=site_type,
            has_data_type_cd=has_data_type_cd,
        )

        return location_info, stations

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

    def _parse_rdb_sites_response(
        self,
        rdb_text: str,
        search_lat: float,
        search_lon: float,
    ) -> list[USGSStationSummary]:
        """Parse the RDB response from the USGS Site Service API."""
        stations = []
        lines = rdb_text.strip().split("\n")

        header_idx = None
        for i, line in enumerate(lines):
            if not line.startswith("#") and line.strip():
                header_idx = i
                break

        if header_idx is None:
            return stations

        headers = lines[header_idx].split("\t")

        data_start_idx = header_idx + 2

        for line in lines[data_start_idx:]:
            if not line.strip() or line.startswith("#"):
                continue

            fields = line.split("\t")

            site_data = {}
            for i, header in enumerate(headers):
                if i < len(fields):
                    site_data[header] = fields[i].strip()

            site_no = site_data.get("site_no", "")
            site_name = site_data.get("station_nm", "")
            site_type = site_data.get("site_tp_cd", "")

            try:
                lat_str = site_data.get("dec_lat_va", "0")
                lon_str = site_data.get("dec_long_va", "0")
                lat = float(lat_str) if lat_str else 0
                lon = float(lon_str) if lon_str else 0
            except (ValueError, TypeError):
                continue

            if lat == 0 or lon == 0:
                continue

            distance_miles = None
            if lat and lon:
                distance_miles = geodesic((search_lat, search_lon), (lat, lon)).miles

            state_cd = site_data.get("state_cd")
            county_cd = site_data.get("county_cd")
            huc_cd = site_data.get("huc_cd")
            elevation_ft = None
            elev_str = site_data.get("alt_va", "")
            if elev_str and elev_str != "":
                with contextlib.suppress(ValueError, TypeError):
                    elevation_ft = float(elev_str)
                    pass

            station = USGSStationSummary(
                site_no=site_no,
                site_name=site_name,
                site_type=site_type,
                latitude=lat,
                longitude=lon,
                state_cd=state_cd,
                county_cd=county_cd,
                huc_cd=huc_cd,
                elevation_ft=elevation_ft,
                available_parameters=[],
                distance_miles=distance_miles,
            )

            if site_no:
                stations.append(station)

        stations.sort(key=lambda s: s.distance_miles or float("inf"))
        return stations
