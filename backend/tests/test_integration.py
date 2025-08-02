"""Integration tests for the precipitation API endpoints"""

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


class TestPrecipitationAPI:
    """Test precipitation API endpoints with real USGS data"""

    def test_health_check(self):
        """Test the health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy"}

    def test_root_endpoint(self):
        """Test the root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        assert "PrecipAPI" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_find_precipitation_stations_denver(self):
        """Test finding precipitation stations near Denver"""
        latitude = 39.7392
        longitude = -104.9903

        response = client.get(
            "/api/precipitation/stations",
            params={
                "latitude": latitude,
                "longitude": longitude,
                "radius_miles": 50,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "location" in data
        assert "stations" in data
        assert "count" in data
        assert "radius_miles" in data

        # Verify location data
        assert data["location"]["latitude"] == latitude
        assert data["location"]["longitude"] == longitude
        assert data["radius_miles"] == 50

        # Should find at least some stations near Denver
        assert isinstance(data["stations"], list)
        assert data["count"] == len(data["stations"])

        # If stations found, verify station structure
        if data["stations"]:
            station = data["stations"][0]
            required_fields = [
                "site_no",
                "site_name",
                "site_type",
                "latitude",
                "longitude",
                "available_parameters",
            ]
            for field in required_fields:
                assert field in station

            # Verify coordinates are reasonable (within Colorado)
            assert 36 <= station["latitude"] <= 42  # Colorado latitude range
            assert -110 <= station["longitude"] <= -102  # Colorado longitude range
