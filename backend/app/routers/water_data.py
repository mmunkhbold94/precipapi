"""Water Data API endpoints for streamflow and precipitation."""

from fastapi import APIRouter, HTTPException, Query

from app.integrations.usgs.client import USGSClient
from app.integrations.usgs.models import USGSDefaults, USGSTimePeriod

router = APIRouter(prefix="/water", tags=["water-data"])

PRECIP_PERIOD_QUERY = Query(
    USGSDefaults.DEFAULT_PERIOD,
    description="Time period for data retrieval (e.g., P7D for last 7 days)",
)

STREAM_PERIOD_QUERY = Query(
    USGSDefaults.DEFAULT_PERIOD,
    description="Time period for data retrieval (e.g., P7D for last 7 days)",
)


@router.get("/streamflow/{site_no}")
async def get_streamflow_data(
    site_no: str,
    period: USGSTimePeriod = STREAM_PERIOD_QUERY,
):
    """Get streamflow data for a specific USGS site."""
    try:
        async with USGSClient() as client:
            measurements = await client.get_streamflow_data(
                site_codes=[site_no], period=period
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
            detail=f"Error fetching streamflow data for site {site_no}: (e)",
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
                site_codes=[site_no], period=period
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
                site_codes=[site_no], period=USGSTimePeriod.LAST_DAY
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
                site_codes=[site_no], period=USGSTimePeriod.LAST_DAY
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
                    site_codes=[site_no], period=USGSTimePeriod.LAST_DAY
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
                    site_codes=[site_no], period=USGSTimePeriod.LAST_DAY
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
