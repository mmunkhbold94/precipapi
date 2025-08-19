"""Water Data API endpoints for streamflow and precipitation."""

from fastapi import APIRouter, HTTPException, Query

from app import StationNotFound, create_precipapi
from app.integrations.usgs.client import USGSClient
from app.integrations.usgs.models import USGSDefaults, USGSTimePeriod
from app.models.models import ParameterType

router = APIRouter(prefix="/water", tags=["water-data"])

DEFAULT_PERIOD = USGSDefaults.DEFAULT_PERIOD

PRECIP_PERIOD_QUERY = Query(
    USGSDefaults.DEFAULT_PERIOD,
    description="Time period for data retrieval (e.g., P7D for last 7 days)",
)

STREAM_PERIOD_QUERY = Query(
    USGSDefaults.DEFAULT_PERIOD,
    description="Time period for data retrieval (e.g., P7D for last 7 days)",
)

SOURCES_QUERY = Query(default=["usgs"], description="Data sources to search")

PARAMETER_QUERY = Query(default=None, description="Filter by parameter types")

## Station Discovery


@router.get("/stations")
async def find_stations(
    latitude: float | None = Query(None, description="Latitude for station search"),
    longitude: float | None = Query(None, description="Longitude coordinate"),
    address: str | None = Query(None, description="Address to search near"),
    radius_miles: float = Query(25, description="Search radius in miles"),
    sources: list[str] = SOURCES_QUERY,
    parameter_types: list[str] | None = PARAMETER_QUERY,
    max_results: int = Query(100, description="Maximum number of results"),
):
    """Find USGS monitoring stations by coordinates or address."""
    try:
        if not latitude and not address:
            raise HTTPException(
                status_code=400,
                detail="Either latitude/longitude or address must be provided.",
            )
        if latitude is not None and longitude is None:
            raise HTTPException(
                status_code=400,
                detail="Longitude must be provided if latitude is specified.",
            )
        api = await create_precipapi(sources=sources, timeout=30)

        async with api:
            if latitude is not None and longitude is not None:
                response = await api.find_stations_by_coordinates(
                    latitude=latitude,
                    longitude=longitude,
                    radius_miles=radius_miles,
                    parameter_types=[ParameterType(pt) for pt in parameter_types]
                    if parameter_types
                    else None,
                )
            elif address:
                response = await api.find_stations_by_address(
                    address=address,
                    radius_miles=radius_miles,
                    parameter_types=[ParameterType(pt) for pt in parameter_types]
                    if parameter_types
                    else None,
                )
            else:
                # This case should not be reached due to the initial checks
                raise HTTPException(
                    status_code=400, detail="Invalid request parameters."
                )
        limited_stations = response.stations[:max_results]

        return {
            "location": response.search_location,
            "stations": [station.model_dump() for station in limited_stations],
            "count": len(limited_stations),
            "total_found": response.total_count,
            "radius_miles": response.radius_miles,
            "sources_searched": sources,
            "errors": response.errors_by_source,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error finding stations: {e!s}",
        ) from e


@router.get("/stations/{station_id}")
async def get_station_details(station_id: str):
    """
    Get detailed information about a specific station.
    NEW: Works with unified station IDs (e.g., "usgs:01646500") and provides
    comprehensive station information including available parameters.
    """
    try:
        api = await create_precipapi(timeout=30)

        async with api:
            station = await api.get_station(station_id)

        return station.dict()

    except StationNotFound as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching station details: {e}"
        ) from e


@router.get("/streamflow/{site_no}")
async def get_streamflow_data(
    site_no: str,
    period: USGSTimePeriod = STREAM_PERIOD_QUERY,
):
    """Get streamflow data for a specific USGS site."""
    try:
        async with USGSClient() as client:
            measurements = await client.get_streamflow_data(
                site_codes=[site_no], period=period.value
            )
        return {
            "site_no": site_no,
            "parameter": "streamflow",
            "period": str(period),
            "measurements": [m.dict() for m in measurements],
            "count": len(measurements),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching streamflow data for site {site_no}: {e!s}",
        ) from e


@router.get("/precipitation/{site_no}")
async def get_precipitation_data(
    site_no: str,
    period: USGSTimePeriod = PRECIP_PERIOD_QUERY,
):
    """Get precipitation data for a specific USGS site."""
    try:
        async with USGSClient() as client:
            measurements = await client.get_precipitation_data(
                site_codes=[site_no], period=period.value
            )

        return {
            "site_no": site_no,
            "parameter": "precipitation",
            "period": str(period),
            "measurements": [m.dict() for m in measurements],
            "count": len(measurements),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching precipitation data for site {site_no}: {e!s}",
        ) from e


@router.get("/streamflow/{site_no}/latest")
async def get_latest_streamflow(site_no: str):
    """Get the most recent streamflow measurement for a site."""
    try:
        async with USGSClient() as client:
            measurements = await client.get_streamflow_data(
                site_codes=[site_no], period=USGSTimePeriod.LAST_DAY.value
            )

        if not measurements:
            raise HTTPException(
                status_code=404,
                detail=f"No recent streamflow data found for site {site_no}",
            )

        # Get the most recent measurement
        latest = max(measurements, key=lambda m: m.timestamp)

        return {
            "site_no": site_no,
            "parameter": "streamflow",
            "latest_measurement": latest.dict(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching latest streamflow data for site {site_no}: {e!s}",
        ) from e


@router.get("/precipitation/{site_no}/latest")
async def get_latest_precipitation(site_no: str):
    """Get the most recent precipitation measurement for a site."""
    try:
        async with USGSClient() as client:
            measurements = await client.get_precipitation_data(
                site_codes=[site_no], period=USGSTimePeriod.LAST_DAY.value
            )

        if not measurements:
            raise HTTPException(
                status_code=404,
                detail=f"No recent precipitation data found for site {site_no}",
            )

        # Get the most recent measurement
        latest = max(measurements, key=lambda m: m.timestamp)

        return {
            "site_no": site_no,
            "parameter": "precipitation",
            "latest_measurement": latest.dict(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching latest precipitation data for site {site_no}: {e!s}",
        ) from e


@router.get("/sites/{site_no}/parameters")
async def get_available_parameters(site_no: str):
    """Test what parameters are available for a specific site."""
    try:
        results = {}

        async with USGSClient() as client:
            # Test streamflow
            try:
                streamflow_data = await client.get_streamflow_data(
                    site_codes=[site_no], period=USGSTimePeriod.LAST_DAY.value
                )
                results["streamflow"] = {
                    "available": len(streamflow_data) > 0,
                    "recent_measurements": len(streamflow_data),
                }
            except Exception:
                results["streamflow"] = {
                    "available": False,
                    "error": "No streamflow data",
                }

            # Test precipitation
            try:
                precip_data = await client.get_precipitation_data(
                    site_codes=[site_no], period=USGSTimePeriod.LAST_DAY.value
                )
                results["precipitation"] = {
                    "available": len(precip_data) > 0,
                    "recent_measurements": len(precip_data),
                }
            except Exception:
                results["precipitation"] = {
                    "available": False,
                    "error": "No precipitation data",
                }

        return {
            "site_no": site_no,
            "parameters": results,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error checking parameters for site {site_no}: {e!s}",
        ) from e


@router.get("/stations/search")
async def search_stations_by_address(
    address: str = Query(..., description="Address to search for USGS stations"),
    radius_miles: float = Query(50, description="Search radius in miles"),
    site_type: str | None = Query(
        None, description="Type of site filter (ST=stream, GW=groundwater, etc.)"
    ),
    limit: int = Query(50, description="Maximum number of results to return"),
):
    """Search for USGS stations by address."""
    try:
        async with USGSClient() as client:
            location_info, stations = await client.get_sites_by_address(
                address=address,
                radius_miles=radius_miles,
                site_type=site_type,
                has_data_type_cd="iv",
            )
        limited_stations = stations[:limit]

        return {
            "location": location_info,
            "search_parameters": {
                "address": address,
                "radius_miles": radius_miles,
                "site_type": site_type,
                "limit": limit,
            },
            "stations": [stations.dict() for stations in limited_stations],
            "count": len(limited_stations),
            "total_count": len(stations),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error searching for stations by address '{address}': {e!s}",
        ) from None


@router.get("/stations/coordinates")
async def search_stations_by_coordinates(
    latitude: float = Query(..., description="Latitude"),
    longitude: float = Query(..., description="Longitude"),
    radius_miles: float = Query(50, description="Search radius in miles"),
    site_type: str | None = Query(None, description="Site type filter"),
    limit: int = Query(50, description="Maximum number of stations to return"),
):
    """Find USGS monitoring stations near coordinates."""
    try:
        async with USGSClient() as client:
            stations = await client.get_sites_by_coordinates(
                latitude=latitude,
                longitude=longitude,
                radius_miles=radius_miles,
                site_type=site_type,
                has_data_type_cd="iv",
            )

        # Limit results
        limited_stations = stations[:limit]

        return {
            "location": {
                "latitude": latitude,
                "longitude": longitude,
            },
            "search_parameters": {
                "radius_miles": radius_miles,
                "site_type": site_type,
                "limit": limit,
            },
            "stations": [station.dict() for station in limited_stations],
            "count": len(limited_stations),
            "total_found": len(stations),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error searching for stations: {e!s}",
        ) from None
