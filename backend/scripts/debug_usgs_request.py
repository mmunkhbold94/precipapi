#!/usr/bin/env python3
"""Debug script to test USGS API requests directly."""

import asyncio

import httpx

from app.integrations.usgs.models import USGSDefaults, USGSParameterCode


async def test_usgs_request():
    """Test the USGS API request directly."""
    base_url = "https://waterservices.usgs.gov/nwis"

    # Test parameters
    params = {
        "format": "json",
        "sites": "01646500",
        "parameterCd": USGSParameterCode.STREAMFLOW,
        "period": str(USGSDefaults.DEFAULT_PERIOD),
    }

    print(f"Testing USGS API with params: {params}")

    async with httpx.AsyncClient(timeout=30) as client:
        url = f"{base_url}/iv/"
        print(f"Making request to: {url}")

        try:
            response = await client.get(url, params=params)
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {response.headers}")
            print(f"Response text: {response.text[:500]}...")

            if response.status_code == 200:
                data = response.json()
                print(f"Response data keys: {list(data.keys())}")
            else:
                print(f"Error response: {response.text}")

        except Exception as e:
            print(f"Exception: {e}")


if __name__ == "__main__":
    asyncio.run(test_usgs_request())
