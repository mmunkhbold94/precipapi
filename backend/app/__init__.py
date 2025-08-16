# precipapi/__init__.py
"""Main PrecipAPI with polymorphic connector pattern."""

import asyncio

from app.base import DataSourceConnector
from app.integrations.usgs.client import USGSConnector
from app.models.exceptions import DataSourceError, StationNotFound
from app.models.models import (
    DataResponse,
    DataSource,
    ParameterType,
    Station,
    StationSearchRequest,
    StationSearchResponse,
    TimeInterval,
)

# Available connectors registry
AVAILABLE_CONNECTORS = {
    DataSource.USGS: USGSConnector,
    # Add more as you implement them:
    # DataSource.NOAA: NOAAConnector,
    # DataSource.MESONET: MesonetConnector,
}


class PrecipAPI:
    """
    Main API class providing unified access to multiple precipitation data sources.
    This replaces your individual endpoint functions and provides a consistent
    interface across all data sources with automatic error handling and aggregation.
    """

    def __init__(self, sources: list[DataSource] | None = None, **connector_kwargs):
        """
        Initialize PrecipAPI with specified data sources.
        Args:
            sources: List of data sources to enable. If None, enables all available.
            **connector_kwargs: Passed to connector initialization (timeout, etc.)
        """
        if sources is None:
            sources = list(AVAILABLE_CONNECTORS.keys())

        self.connectors = {}
        self.errors = {}

        for source in sources:
            if source in AVAILABLE_CONNECTORS:
                try:
                    connector_class = AVAILABLE_CONNECTORS[source]
                    self.connectors[source] = connector_class(**connector_kwargs)
                except Exception as e:
                    self.errors[source.value] = (
                        f"Failed to initialize {source.value}: {e}"
                    )
            else:
                self.errors[source.value] = (
                    f"Connector for {source.value} not available"
                )

    async def find_stations(
        self, request: StationSearchRequest
    ) -> StationSearchResponse:
        """
        Find stations across all enabled data sources.
        This replaces your existing station search endpoints and aggregates
        results from multiple sources automatically.
        """
        if not request.latitude and not request.address:
            raise ValueError("Either latitude/longitude or address must be provided")

        all_stations = []
        errors_by_source = {}

        # Filter connectors if specific sources requested
        active_connectors = self.connectors
        if request.sources:
            active_connectors = {
                source: connector
                for source, connector in self.connectors.items()
                if source in request.sources
            }

        # Fan out to all connectors
        async def search_source(source: DataSource, connector: DataSourceConnector):
            try:
                if request.latitude is not None and request.longitude is not None:
                    stations = connector.find_stations_by_coordinates(
                        latitude=request.latitude,
                        longitude=request.longitude,
                        radius_miles=request.radius_miles,
                        parameter_types=request.parameter_types,
                    )
                else:
                    if request.address is None:
                        raise ValueError(
                            "Address must be provided for address-based station search"
                        )
                    stations = connector.find_stations_by_address(
                        address=request.address,
                        radius_miles=request.radius_miles,
                        parameter_types=request.parameter_types,
                    )
                return source, stations, None
            except Exception as e:
                return source, [], str(e)

        # Execute searches concurrently
        tasks = [
            search_source(source, connector)
            for source, connector in active_connectors.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for result in results:
            # Check if the result is an exception first
            if isinstance(result, BaseException):
                errors_by_source["unknown"] = str(result)
                continue

            # Now we know it's a tuple and can safely unpack
            source, stations, error = result
            if error:
                errors_by_source[source.value] = error
            else:
                all_stations.extend(stations)

        # Deduplicate stations
        unique_stations = self._deduplicate_stations(all_stations)

        # Apply limits
        if request.max_results:
            unique_stations = unique_stations[: request.max_results]

        # Combine with initialization errors
        all_errors = {**self.errors, **errors_by_source}

        return StationSearchResponse(
            stations=unique_stations,
            total_count=len(unique_stations),
            search_location={
                "latitude": request.latitude,
                "longitude": request.longitude,
                "address": request.address,
            },
            radius_miles=request.radius_miles,
            errors_by_source=all_errors,
        )

    async def get_station(self, station_id: str) -> Station:
        """
        Get detailed information about a specific station.
        This replaces individual source-specific station info endpoints.
        """
        source_name, vendor_id = self._decode_station_id(station_id)

        try:
            source = DataSource(source_name)
        except ValueError:
            raise StationNotFound(f"Unknown data source: {source_name}") from None

        if source not in self.connectors:
            raise StationNotFound(f"Data source {source_name} not available")

        connector = self.connectors[source]
        return await connector.get_station_info(vendor_id)

    async def get_precipitation_data(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        interval: TimeInterval = TimeInterval.DAY,
    ) -> DataResponse:
        """
        Get precipitation data for a station.
        This replaces your precipitation data endpoints and works across all sources.
        """
        source_name, vendor_id = self._decode_station_id(station_id)

        try:
            source = DataSource(source_name)
        except ValueError:
            raise StationNotFound(f"Unknown data source: {source_name}") from None

        if source not in self.connectors:
            raise DataSourceError(f"Data source {source_name} not available")

        connector = self.connectors[source]
        measurements = await connector.get_precipitation_data(
            vendor_id, start_date, end_date, interval
        )

        return DataResponse(
            station_id=station_id,
            parameter_type=ParameterType.PRECIPITATION,
            measurements=measurements,
            total_count=len(measurements),
            date_range={"start": start_date, "end": end_date},
            metadata={"interval": interval.value, "source": source.value},
        )

    async def get_streamflow_data(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        interval: TimeInterval = TimeInterval.DAY,
    ) -> DataResponse:
        """
        Get streamflow data for a station.
        This replaces your streamflow data endpoints and works across all sources.
        """
        source_name, vendor_id = self._decode_station_id(station_id)

        try:
            source = DataSource(source_name)
        except ValueError:
            raise StationNotFound(f"Unknown data source: {source_name}") from None

        if source not in self.connectors:
            raise DataSourceError(f"Data source {source_name} not available")

        connector = self.connectors[source]
        measurements = await connector.get_streamflow_data(
            vendor_id, start_date, end_date, interval
        )

        return DataResponse(
            station_id=station_id,
            parameter_type=ParameterType.STREAMFLOW,
            measurements=measurements,
            total_count=len(measurements),
            date_range={"start": start_date, "end": end_date},
            metadata={"interval": interval.value, "source": source.value},
        )

    # Convenience methods for backward compatibility
    async def find_stations_by_coordinates(
        self,
        latitude: float,
        longitude: float,
        radius_miles: float = 25,
        parameter_types: list[ParameterType] | None = None,
        sources: list[DataSource] | None = None,
    ) -> StationSearchResponse:
        """Backward-compatible coordinates search."""
        request = StationSearchRequest(
            latitude=latitude,
            longitude=longitude,
            radius_miles=radius_miles,
            parameter_types=parameter_types,
            sources=sources,
        )
        return await self.find_stations(request)

    async def find_stations_by_address(
        self,
        address: str,
        radius_miles: float = 25,
        parameter_types: list[ParameterType] | None = None,
        sources: list[DataSource] | None = None,
    ) -> StationSearchResponse:
        """Backward-compatible address search."""
        request = StationSearchRequest(
            address=address,
            radius_miles=radius_miles,
            parameter_types=parameter_types,
            sources=sources,
        )
        return await self.find_stations(request)

    # Private helper methods
    def _decode_station_id(self, station_id: str) -> tuple[str, str]:
        """Decode station ID into source and vendor ID."""
        try:
            source, vendor_id = station_id.split(":", 1)
            return source, vendor_id
        except ValueError:
            raise ValueError(
                f"Invalid station ID format: {station_id}. Expected 'source:id'"
            ) from None

    def _deduplicate_stations(self, stations: list[Station]) -> list[Station]:
        """Remove duplicate stations that may exist across sources."""
        seen = set()
        unique = []

        for station in stations:
            # Create key based on coordinates (rounded) and name
            key = (
                round(station.latitude, 4),
                round(station.longitude, 4),
                station.name.lower().strip(),
            )

            if key not in seen:
                seen.add(key)
                unique.append(station)

        # Sort by distance if available
        def sort_key(s):
            return (s.distance_miles or float("inf"), s.name)

        return sorted(unique, key=sort_key)

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup connectors."""
        cleanup_tasks = []
        for connector in self.connectors.values():
            if hasattr(connector, "__aexit__"):
                cleanup_tasks.append(connector.__aexit__(exc_type, exc_val, exc_tb))

        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)


# Convenience factory functions for easy migration
async def create_precipapi(
    sources: list[str] | None = None, **connector_kwargs
) -> PrecipAPI:
    """
    Factory function to create PrecipAPI instance.
    Args:
        sources: List of source names ("usgs", "noaa", etc.)
        **connector_kwargs: Passed to connectors (timeout, etc.)
    """
    source_enums = [DataSource(s) for s in sources] if sources else None

    return PrecipAPI(sources=source_enums, **connector_kwargs)
