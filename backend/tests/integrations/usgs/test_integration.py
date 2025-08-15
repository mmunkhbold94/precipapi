"""Unit tests for USGS Water Services API client."""

import pytest

from app.integrations.usgs.client import (
    USGSClient,
)
from app.integrations.usgs.models import (
    USGSStationSummary,
)


class TestUSGSClient:
    """Test the USGS client methods with real API calls."""

    @pytest.fixture
    def client(self):
        """Create a USGS client for testing."""
        return USGSClient(timeout=30)

    # @pytest.mark.asyncio
    # async def test_find_stations_by_location_potomac(self, client):
    #     """Test finding stations near Denver, CO."""
    #     async with client as usgs:
    #         response = await usgs.get_instantaneous_values(
    #             site_codes=["01646500"],  # Potomac River
    #             parameter_codes=["00060"],  # Streamflow
    #             period="P1D",
    #         )

    #     # Verify we get a proper USGSSiteInfo object
    #     assert isinstance(response, USGSInstantaneousValuesResponse)
    #     assert len(response.time_series) > 0

    #     # Verify site info
    #     ts = response.time_series[0]
    #     assert ts.source_info.site_no == "01646500"
    #     assert "POTOMAC" in ts.source_info.site_name.upper()

    # @pytest.mark.asyncio
    # async def test_get_streamflow_data_potomac(self, client):
    #     """Test processing streamflow data from Potomac River."""
    #     async with client as usgs:
    #         measurements = await usgs.get_streamflow_data(
    #             site_codes=["01646500"], period="P2D"
    #         )

    #     # Should have measurements
    #     assert len(measurements) > 0

    #     # Verify measurement structure
    #     measurement = measurements[0]
    #     assert isinstance(measurement, StreamflowMeasurement)
    #     assert measurement.site_no == "01646500"
    #     assert "POTOMAC" in measurement.site_name.upper()
    #     assert measurement.value is not None
    #     assert measurement.unit == "ft3/s"
    #     assert isinstance(measurement.timestamp, datetime)

    # @pytest.mark.asyncio
    # async def test_get_precipitation_data(self, client):
    #     """Test processing precipitation data from a known site."""
    #     # Using USGS site 01646500 (Potomac River) - it may or may not have precipitation data
    #     # but the test will verify the method works correctly either way
    #     async with client as usgs:
    #         measurements = await usgs.get_precipitation_data(
    #             site_codes=["01646500"],
    #             period="P7D",  # Longer period to increase chance of finding data
    #         )

    #     # The method should work without errors even if no precipitation data is available
    #     assert isinstance(measurements, list)

    #     # If measurements are found, verify their structure
    #     if measurements:
    #         measurement = measurements[0]
    #         assert isinstance(measurement, PrecipitationMeasurement)
    #         assert measurement.site_no == "01646500"
    #         assert "POTOMAC" in measurement.site_name.upper()
    #         # Precipitation value can be None (no rain) or a float
    #         assert measurement.value is None or isinstance(
    #             measurement.value, int | float
    #         )
    #         # Common precipitation units
    #         assert measurement.unit in ["in", "inches", "mm", "millimeters", "ft3/s"]
    #         assert isinstance(measurement.timestamp, datetime)
    #     else:
    #         # No precipitation data found - this is normal for many sites
    #         print(
    #             "No precipitation data found for this site/period - this is expected for many USGS sites"
    #         )

    # @pytest.mark.asyncio
    # async def test_calculate_distance(self, client):
    #     """Test distance calculation."""
    #     # Distance between NYC and Philadelphia (roughly 95 miles)
    #     nyc_lat, nyc_lon = 40.7128, -74.0060
    #     philly_lat, philly_lon = 39.9526, -75.1652

    #     distance = client._calculate_distance(nyc_lat, nyc_lon, philly_lat, philly_lon)

    #     # Should be roughly 95 miles (within reasonable margin)
    #     assert 80 <= distance <= 110

    @pytest.mark.asyncio
    async def test_get_sites_by_coordinates_denver(self, client):
        """Test finding stations near Denver, CO by coordinates."""
        denver_lat = 39.7392
        denver_lon = -104.9903

        async with client as usgs:
            stations = await usgs.get_sites_by_coordinates(
                latitude=denver_lat,
                longitude=denver_lon,
                radius_miles=25,
                site_type="ST",  # Stream sites only
            )

        # Should find some stream monitoring stations near Denver
        assert len(stations) > 0
        assert len(stations) <= 499

        # Verify station structure
        station = stations[0]
        assert isinstance(station, USGSStationSummary)
        assert station.site_no
        assert station.site_name
        assert station.latitude != 0
        assert station.longitude != 0
        assert station.distance_miles is not None
        assert station.site_type == "ST"

        # Verify distance calculation - should be within search radius
        assert station.distance_miles <= 25

        # Verify coordinates are reasonable for Colorado area
        assert 36 <= station.latitude <= 42
        assert -110 <= station.longitude <= -102

        # Verify stations are sorted by distance
        distances = [s.distance_miles for s in stations[:5]]
        assert distances == sorted(distances)

        print(f"Found {len(stations)} stream stations near Denver")
        print(
            f"Closest station: {station.site_name} ({station.site_no}) at {station.distance_miles:.1f} miles"
        )

    # @pytest.mark.asyncio
    # async def test_get_sites_by_coordinates_all_types(self, client):
    #     """Test finding all types of stations (not just streams)."""
    #     async with client as usgs:
    #         stations = await usgs.get_sites_by_coordinates(
    #             latitude=39.7392,  # Denver
    #             longitude=-104.9903,
    #             radius_miles=15,
    #             # No site_type filter to get all types
    #         )

    #     assert len(stations) > 0

    #     # Should find multiple types of monitoring sites
    #     site_types = {station.site_type for station in stations}
    #     assert len(site_types) > 1  # Should have variety

    #     print(f"Found site types: {sorted(site_types)}")

    # @pytest.mark.asyncio
    # async def test_get_sites_by_address_denver(self, client):
    #     """Test finding stations by address."""
    #     async with client as usgs:
    #         location_info, stations = await usgs.get_sites_by_address(
    #             address="Denver, CO",
    #             radius_miles=20,
    #             site_type="ST",
    #         )

    #     # Verify location was geocoded correctly
    #     assert location_info['address'] == "Denver, CO"
    #     assert 'resolved_address' in location_info
    #     assert 'latitude' in location_info
    #     assert 'longitude' in location_info

    #     # Should be near Denver coordinates (within 1 degree)
    #     assert abs(location_info['latitude'] - 39.7392) < 1.0
    #     assert abs(location_info['longitude'] - (-104.9903)) < 1.0

    #     # Should find stations
    #     assert len(stations) > 0

    #     # Verify station structure
    #     station = stations[0]
    #     assert isinstance(station, USGSStationSummary)
    #     assert station.site_type == "ST"
    #     # assert station.distance_miles <= 20

    #     print(f"Geocoded '{location_info['address']}' to: {location_info['resolved_address']}")
    #     print(f"Coordinates: {location_info['latitude']:.4f}, {location_info['longitude']:.4f}")
    #     print(f"Found {len(stations)} stations within 20 miles")

    # @pytest.mark.asyncio
    # async def test_get_sites_by_address_specific_location(self, client):
    #     """Test with a more specific address."""
    #     async with client as usgs:
    #         location_info, stations = await usgs.get_sites_by_address(
    #             address="Boulder, Colorado",
    #             radius_miles=15,
    #         )

    #     # Should be near Boulder (different from Denver)
    #     assert abs(location_info['latitude'] - 40.0150) < 0.5  # Boulder coordinates
    #     assert abs(location_info['longitude'] - (-105.2705)) < 0.5

    #     # Should find some stations
    #     assert len(stations) >= 0  # May be 0 in some areas, that's ok

    #     print(f"Boulder geocoded to: {location_info['latitude']:.4f}, {location_info['longitude']:.4f}")

    # @pytest.mark.asyncio
    # async def test_rdb_parsing_comprehensive(self, client):
    #     """Test that RDB parsing handles various site types and edge cases."""
    #     async with client as usgs:
    #         stations = await usgs.get_sites_by_coordinates(
    #             latitude=40.7128,  # NYC area - lots of variety
    #             longitude=-74.0060,
    #             radius_miles=50,
    #             # No filters to get maximum variety
    #         )

    #     # Should find stations
    #     assert len(stations) > 0

    #     # Test for various data fields being parsed correctly
    #     valid_stations = []
    #     for station in stations:
    #         # All stations should have basic required fields
    #         assert station.site_no
    #         assert station.site_name
    #         assert station.latitude != 0
    #         assert station.longitude != 0
    #         assert station.distance_miles is not None

    #         # Some may have optional fields
    #         if station.elevation_ft is not None:
    #             assert isinstance(station.elevation_ft, int | float)

    #         valid_stations.append(station)

    #     # Group by site type to see variety
    #     site_types = {}
    #     for station in valid_stations:
    #         site_type = station.site_type
    #         if site_type not in site_types:
    #             site_types[site_type] = []
    #         site_types[site_type].append(station)

    #     print(f"Found {len(valid_stations)} valid stations")
    #     print(f"Site types found: {list(site_types.keys())}")
    #     for site_type, sites in site_types.items():
    #         print(f"  {site_type}: {len(sites)} sites")

    # @pytest.mark.asyncio
    # async def test_invalid_address_handling(self, client):
    #     """Test handling of invalid addresses."""
    #     async with client as usgs:
    #         with pytest.raises(Exception):  # Should raise USGSException
    #             await usgs.get_sites_by_address(
    #                 address="ThisIsNotARealAddressAtAll12345XYZ",
    #                 radius_miles=10,
    #             )

    # @pytest.mark.asyncio
    # async def test_small_radius_search(self, client):
    #     """Test with a very small radius to ensure distance filtering works."""
    #     async with client as usgs:
    #         stations = await usgs.get_sites_by_coordinates(
    #             latitude=39.7392,  # Denver
    #             longitude=-104.9903,
    #             radius_miles=5,  # Very small radius
    #             site_type="ST",
    #         )

    #     # All stations should be within the small radius
    #     for station in stations:
    #         assert station.distance_miles <= 5

    #     print(f"Found {len(stations)} stations within 5 miles of Denver")

    # @pytest.mark.asyncio
    # async def test_empty_result_handling(self, client):
    #     """Test handling when no stations are found."""
    #     async with client as usgs:
    #         # Search in middle of ocean where no stations should exist
    #         stations = await usgs.get_sites_by_coordinates(
    #             latitude=30.0,  # Middle of Atlantic
    #             longitude=-50.0,
    #             radius_miles=10,
    #         )

    #     # Should return empty list, not error
    #     assert isinstance(stations, list)
    #     assert len(stations) == 0
