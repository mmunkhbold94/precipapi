"""USGS Water Services API client."""

import asyncio
import contextlib
import math
from datetime import datetime
from functools import partial
from typing import Any

import httpx
import requests.auth
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from requests import Session

from app.base import DataSourceConnector
from app.models.exceptions import DataSourceError, StationNotFound
from app.models.models import DataSource, ParameterType, Station, TimeInterval
from app.models.models import PrecipitationMeasurement as StandardPrecipMeasurement
from app.models.models import StreamflowMeasurement as StandardStreamflowMeasurement

from .models import (
    PrecipitationMeasurement,
    StreamflowMeasurement,
    USGSDefaults,
    USGSInstantaneousValuesResponse,
    USGSParameterCode,
    USGSSiteType,
    USGSStationSummary,
    USGSTimePeriod,
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
        # USGS Site Service uses bounding box, not lat/long + radius
        # Calculate bounding box from center point and radius
        # Rough conversion: 1 degree latitude ≈ 69 miles
        lat_delta = radius_miles / 69.0
        # Longitude degrees vary with latitude, but this is a reasonable approximation
        lon_delta = radius_miles / (69.0 * abs(math.cos(math.radians(latitude))))

        # Bounding box format: west,south,east,north
        # Round to 7 decimal places as required by USGS API
        west = round(longitude - lon_delta, 7)
        south = round(latitude - lat_delta, 7)
        east = round(longitude + lon_delta, 7)
        north = round(latitude + lat_delta, 7)
        params = {
            "format": "rdb",  # Site Service API has RDB (tab-delimited) format
            "bBox": f"{west},{south},{east},{north}",
            "siteOutput": "expanded",
        }
        if site_type:
            params["siteType"] = site_type
        if has_data_type_cd:
            params["hasDataTypeCd"] = has_data_type_cd

        response_text = await self._request_text("/site/", params=params)

        stations = self._parse_rdb_sites_response(response_text, latitude, longitude)
        # Filter by actual distance since bounding box is rectangular
        filtered_stations = []
        for station in stations:
            if (
                station.distance_miles is not None
                and station.distance_miles <= radius_miles
            ):
                filtered_stations.append(station)

        return filtered_stations

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


class USGSConnector(DataSourceConnector):
    """USGS Water Services API connector implementing standardized interface."""

    @classmethod
    def name(cls) -> str:
        """Return canonical name for USGS data source."""
        return "usgs"

    @classmethod
    def init_from_request_context(cls, **kwargs: Any) -> "USGSConnector":
        """Initialize from request context."""
        return cls(
            timeout=kwargs.get("timeout", 30),
            session=kwargs.get("session"),
            auth=kwargs.get("auth"),
        )

    def __init__(
        self,
        auth: requests.auth.AuthBase | None = None,
        session: Session | None = None,
        timeout: int = 30,
        **kwargs,
    ) -> None:
        """Initialize USGS connector."""
        super().__init__(auth, session)
        self.timeout = timeout
        self._client = None

    async def _get_client(self) -> USGSClient:
        """Get or create USGS client."""
        if self._client is None:
            self._client = USGSClient(timeout=self.timeout)
        return self._client

    # STATION DISCOVERY

    async def find_stations_by_coordinates(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float,
        parameter_types: list[ParameterType] | None = None,
    ) -> list[Station]:
        """Find USGS stations by coordinates."""
        try:
            client = await self._get_client()

            async with client as usgs:
                # Convert parameter types to USGS site types if needed
                site_type = self._parameter_types_to_site_type(parameter_types)

                usgs_stations = await usgs.get_sites_by_coordinates(
                    latitude=latitude,
                    longitude=longitude,
                    radius_miles=radius_miles,
                    site_type=site_type,
                )

            # Convert to standardized Station objects
            stations = []
            for usgs_station in usgs_stations:
                # Add available parameters by querying what data exists
                usgs_station.available_parameters = (
                    await self._get_available_parameters(usgs_station.site_no)
                )
                station = self._convert_usgs_station_to_station(usgs_station)
                stations.append(station)

            return stations

        except USGSException as e:
            raise DataSourceError(f"USGS API error: {e}") from e
        except Exception as e:
            raise DataSourceError(f"Unexpected USGS error: {e}") from e

    async def find_stations_by_address(
        self,
        address: str,
        radius_miles: float,
        parameter_types: list[ParameterType] | None = None,
    ) -> list[Station]:
        """Find USGS stations by address."""
        try:
            client = await self._get_client()

            async with client as usgs:
                site_type = self._parameter_types_to_site_type(parameter_types)

                location_info, usgs_stations = await usgs.get_sites_by_address(
                    address=address,
                    radius_miles=radius_miles,
                    site_type=site_type,
                )

            # Convert to standardized Station objects
            stations = []
            for usgs_station in usgs_stations:
                usgs_station.available_parameters = (
                    await self._get_available_parameters(usgs_station.site_no)
                )
                station = self._convert_usgs_station_to_station(usgs_station)
                stations.append(station)

            return stations

        except USGSException as e:
            raise DataSourceError(f"USGS API error: {e}") from e

    async def get_station_info(self, station_id: str) -> Station:
        """Get detailed USGS station information."""
        try:
            client = await self._get_client()

            async with client as usgs:
                # Get basic station info by doing a site search with specific site number
                params = {
                    "format": "rdb",
                    "sites": station_id,
                    "siteOutput": "expanded",
                }
                response_text = await usgs._request_text("/site/", params=params)
                usgs_stations = usgs._parse_rdb_sites_response(response_text, 0, 0)

                if not usgs_stations:
                    raise StationNotFound(f"USGS station {station_id} not found")

                usgs_station = usgs_stations[0]
                usgs_station.available_parameters = (
                    await self._get_available_parameters(station_id)
                )

                return self._convert_usgs_station_to_station(usgs_station)

        except USGSException as e:
            if "not found" in str(e).lower():
                raise StationNotFound(f"USGS station {station_id} not found") from e
            raise DataSourceError(f"USGS API error: {e}") from e

    # DATA RETRIEVAL

    async def get_precipitation_data(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        interval: TimeInterval,
    ) -> list[PrecipitationMeasurement]:
        """Get precipitation data from USGS."""
        try:
            client = await self._get_client()

            # Convert time interval to USGS period
            period = self._interval_to_period(start_date, end_date, interval)

            async with client as usgs:
                usgs_measurements = await usgs.get_precipitation_data(
                    site_codes=[station_id],
                    period=period,
                )

            # Convert to standardized measurements
            measurements = []
            for usgs_measurement in usgs_measurements:
                measurement = self._convert_usgs_precipitation_to_measurement(
                    usgs_measurement
                )
                measurements.append(measurement)

            return measurements

        except USGSException as e:
            raise DataSourceError(f"USGS precipitation data error: {e}") from e

    async def get_streamflow_data(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        interval: TimeInterval,
    ) -> list[StreamflowMeasurement]:
        """Get streamflow data from USGS."""
        try:
            client = await self._get_client()

            period = self._interval_to_period(start_date, end_date, interval)

            async with client as usgs:
                usgs_measurements = await usgs.get_streamflow_data(
                    site_codes=[station_id],
                    period=period,
                )

            # Convert to standardized measurements
            measurements = []
            for usgs_measurement in usgs_measurements:
                measurement = self._convert_usgs_streamflow_to_measurement(
                    usgs_measurement
                )
                measurements.append(measurement)

            return measurements

        except USGSException as e:
            raise DataSourceError(f"USGS streamflow data error: {e}") from e

    # HELPER METHODS

    def _parameter_types_to_site_type(
        self, parameter_types: list[ParameterType] | None
    ) -> str | None:
        """Convert parameter types to USGS site type filter."""
        if not parameter_types:
            return None

        # Simple mapping - you might want more sophisticated logic
        if ParameterType.STREAMFLOW in parameter_types:
            return USGSSiteType.STREAM.value
        if ParameterType.PRECIPITATION in parameter_types:
            return None  # Don't filter by site type for precipitation

        return None

    def _interval_to_period(
        self, start_date: str, end_date: str, interval: TimeInterval
    ) -> str:
        """Convert interval and date range to USGS period string."""
        # For now, use a simple period mapping
        # You might want more sophisticated logic based on date range

        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        delta = end_dt - start_dt

        if delta.days <= 1:
            return USGSTimePeriod.LAST_DAY.value
        elif delta.days <= 7:
            return USGSTimePeriod.LAST_WEEK.value
        elif delta.days <= 30:
            return USGSTimePeriod.LAST_MONTH.value
        else:
            return USGSTimePeriod.LAST_3_MONTHS.value

    async def _get_available_parameters(self, station_id: str) -> list[str]:
        """Get available parameter codes for a station."""
        try:
            client = await self._get_client()

            # Try common parameters and see what returns data
            available = []
            test_parameters = [
                USGSParameterCode.STREAMFLOW,
                USGSParameterCode.PRECIPITATION,
                USGSParameterCode.GAGE_HEIGHT,
                USGSParameterCode.TEMPERATURE_WATER,
            ]

            async with client as usgs:
                for param in test_parameters:
                    try:
                        response = await usgs.get_instantaneous_values(
                            site_codes=[station_id],
                            parameter_codes=[param.value],
                            period="P1D",  # Just check recent data
                        )
                        if response.time_series:
                            available.append(param.value)
                    except Exception:
                        # Parameter not available for this station
                        continue

            return available

        except Exception:
            # If we can't determine parameters, return empty list
            return []

    def _normalize_parameter_code(self, raw_code: str) -> ParameterType | None:
        """Convert USGS parameter codes to standardized types."""
        mapping = {
            USGSParameterCode.PRECIPITATION.value: ParameterType.PRECIPITATION,
            USGSParameterCode.PRECIPITATION_ACCUMULATED.value: ParameterType.PRECIPITATION,
            USGSParameterCode.STREAMFLOW.value: ParameterType.STREAMFLOW,
            USGSParameterCode.GAGE_HEIGHT.value: ParameterType.GAGE_HEIGHT,
            USGSParameterCode.TEMPERATURE_WATER.value: ParameterType.TEMPERATURE_WATER,
            USGSParameterCode.TEMPERATURE_AIR.value: ParameterType.TEMPERATURE_AIR,
        }
        return mapping.get(raw_code)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client and hasattr(self._client, "__aexit__"):
            # Close the client if it has cleanup methods
            await self._client.__aexit__(exc_type, exc_val, exc_tb)
        # Do not call super().__aexit__ because the base class does not implement it

    # CONVERSION METHODS

    def _convert_usgs_station_to_station(self, usgs_station) -> Station:
        """Convert USGSStationSummary to standardized Station."""
        normalized_params = []
        for param in usgs_station.available_parameters:
            normalized_param = self._normalize_parameter_code(param)
            if normalized_param is not None:
                normalized_params.append(normalized_param)

        return Station(
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
            available_parameters=normalized_params,
            distance_miles=usgs_station.distance_miles,
            metadata={
                "site_type": usgs_station.site_type,
                "state_cd": usgs_station.state_cd,
                "county_cd": usgs_station.county_cd,
                "huc_cd": usgs_station.huc_cd,
            },
        )

    def _convert_usgs_precipitation_to_measurement(self, usgs_measurement):
        """Convert USGS precipitation measurement to standardized format."""
        return StandardPrecipMeasurement(
            station_id=f"usgs:{usgs_measurement.site_no}",
            source=DataSource.USGS,
            vendor_id=usgs_measurement.site_no,
            station_name=usgs_measurement.site_name,
            latitude=usgs_measurement.latitude,
            longitude=usgs_measurement.longitude,
            timestamp=usgs_measurement.timestamp,
            value=float(usgs_measurement.value) if usgs_measurement.value else None,
            unit=usgs_measurement.unit,
            quality_flags=usgs_measurement.qualifiers,
            metadata={"usgs_qualifiers": usgs_measurement.qualifiers},
        )

    def _convert_usgs_streamflow_to_measurement(self, usgs_measurement):
        """Convert USGS streamflow measurement to standardized format."""
        return StandardStreamflowMeasurement(
            station_id=f"usgs:{usgs_measurement.site_no}",
            source=DataSource.USGS,
            vendor_id=usgs_measurement.site_no,
            station_name=usgs_measurement.site_name,
            latitude=usgs_measurement.latitude,
            longitude=usgs_measurement.longitude,
            timestamp=usgs_measurement.timestamp,
            value=float(usgs_measurement.value) if usgs_measurement.value else None,
            unit=usgs_measurement.unit,
            quality_flags=usgs_measurement.qualifiers,
            metadata={"usgs_qualifiers": usgs_measurement.qualifiers},
        )
