"""Unit tests for USGS Water Services API client."""

from datetime import datetime

import pytest

from app.integrations.usgs.client import (
    USGSClient,
)
from app.integrations.usgs.models import (
    PrecipitationMeasurement,
    StreamflowMeasurement,
    USGSInstantaneousValuesResponse,
)


class TestUSGSClient:
    """Test the USGS client methods with real API calls."""

    @pytest.fixture
    def client(self):
        """Create a USGS client for testing."""
        return USGSClient(timeout=30)

    @pytest.mark.asyncio
    async def test_find_stations_by_location_potomac(self, client):
        """Test finding stations near Denver, CO."""
        async with client as usgs:
            response = await usgs.get_instantaneous_values(
                site_codes=["01646500"],  # Potomac River
                parameter_codes=["00060"],  # Streamflow
                period="P1D",
            )

        # Verify we get a proper USGSSiteInfo object
        assert isinstance(response, USGSInstantaneousValuesResponse)
        assert len(response.time_series) > 0

        # Verify site info
        ts = response.time_series[0]
        assert ts.source_info.site_no == "01646500"
        assert "POTOMAC" in ts.source_info.site_name.upper()

    @pytest.mark.asyncio
    async def test_get_streamflow_data_potomac(self, client):
        """Test processing streamflow data from Potomac River."""
        async with client as usgs:
            measurements = await usgs.get_streamflow_data(
                site_codes=["01646500"], period="P2D"
            )

        # Should have measurements
        assert len(measurements) > 0

        # Verify measurement structure
        measurement = measurements[0]
        assert isinstance(measurement, StreamflowMeasurement)
        assert measurement.site_no == "01646500"
        assert "POTOMAC" in measurement.site_name.upper()
        assert measurement.value is not None
        assert measurement.unit == "ft3/s"
        assert isinstance(measurement.timestamp, datetime)

    @pytest.mark.asyncio
    async def test_get_precipitation_data(self, client):
        """Test processing precipitation data from a known site."""
        # Using USGS site 01646500 (Potomac River) - it may or may not have precipitation data
        # but the test will verify the method works correctly either way
        async with client as usgs:
            measurements = await usgs.get_precipitation_data(
                site_codes=["01646500"],
                period="P7D",  # Longer period to increase chance of finding data
            )

        # The method should work without errors even if no precipitation data is available
        assert isinstance(measurements, list)

        # If measurements are found, verify their structure
        if measurements:
            measurement = measurements[0]
            assert isinstance(measurement, PrecipitationMeasurement)
            assert measurement.site_no == "01646500"
            assert "POTOMAC" in measurement.site_name.upper()
            # Precipitation value can be None (no rain) or a float
            assert measurement.value is None or isinstance(
                measurement.value | (int, float)
            )
            # Common precipitation units
            assert measurement.unit in ["in", "inches", "mm", "millimeters", "ft3/s"]
            assert isinstance(measurement.timestamp, datetime)
        else:
            # No precipitation data found - this is normal for many sites
            print(
                "No precipitation data found for this site/period - this is expected for many USGS sites"
            )

    @pytest.mark.asyncio
    async def test_calculate_distance(self, client):
        """Test distance calculation."""
        # Distance between NYC and Philadelphia (roughly 95 miles)
        nyc_lat, nyc_lon = 40.7128, -74.0060
        philly_lat, philly_lon = 39.9526, -75.1652

        distance = client._calculate_distance(nyc_lat, nyc_lon, philly_lat, philly_lon)

        # Should be roughly 95 miles (within reasonable margin)
        assert 80 <= distance <= 110
