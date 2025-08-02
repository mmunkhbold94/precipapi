"""Debug script to examine actual USGS API response structure."""

import asyncio
import json

import httpx


async def debug_usgs_response():
    """Examine the actual USGS API response to understand the structure."""

    base_url = "https://waterservices.usgs.gov/nwis"

    async with httpx.AsyncClient(timeout=30) as client:
        params = {
            "format": "json",
            "sites": "01646500",
            "parameterCd": "00060",
            "period": "P1D",
        }

        url = f"{base_url}/iv"
        print(f"URL: {url}")
        print(f"Params: {params}")

        try:
            response = await client.get(url, params=params)
            print(f"Status: {response.status_code}")

            if response.status_code == 200:
                data = response.json()

                print("\n=== TOP LEVEL STRUCTURE ===")
                print(f"Top level keys: {list(data.keys())}")

                print("\n=== VALUE STRUCTURE ===")
                if "value" in data:
                    value = data["value"]
                    print(f"Value keys: {list(value.keys())}")

                    if "timeSeries" in value:
                        time_series = value["timeSeries"]
                        print(f"Number of time series: {len(time_series)}")

                        if time_series:
                            ts = time_series[0]
                            print("\n=== TIME SERIES STRUCTURE ===")
                            print(f"Time series keys: {list(ts.keys())}")

                            # Examine sourceInfo
                            if "sourceInfo" in ts:
                                source_info = ts["sourceInfo"]
                                print("\n=== SOURCE INFO STRUCTURE ===")
                                print(f"Source info keys: {list(source_info.keys())}")
                                print("Source info sample:")
                                print(json.dumps(source_info, indent=2)[:500] + "...")

                            # Examine variable
                            if "variable" in ts:
                                variable = ts["variable"]
                                print("\n=== VARIABLE STRUCTURE ===")
                                print(f"Variable keys: {list(variable.keys())}")
                                print("Variable sample:")
                                print(json.dumps(variable, indent=2)[:500] + "...")

                            # Examine values
                            if "values" in ts:
                                values = ts["values"]
                                print("\n=== VALUES STRUCTURE ===")
                                print(f"Values type: {type(values)}")
                                print(f"Values length: {len(values)}")
                                if values:
                                    value_group = values[0]
                                    print(
                                        f"Value group keys: {list(value_group.keys())}"
                                    )
                                    if "value" in value_group:
                                        value_list = value_group["value"]
                                        print(f"Value list length: {len(value_list)}")
                                        if value_list:
                                            single_value = value_list[0]
                                            print(
                                                f"Single value keys: {list(single_value.keys())}"
                                            )
                                            print("Single value sample:")
                                            print(json.dumps(single_value, indent=2))

                # Save full response for examination
                with open("usgs_response_sample.json", "w") as f:
                    json.dump(data, f, indent=2)
                print("\nFull response saved to usgs_response_sample.json")

            else:
                print(f"Error response: {response.text}")

        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(debug_usgs_response())
