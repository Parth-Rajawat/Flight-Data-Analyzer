import requests
import json
import os

def fetch_flight_data(api_key: str, limit: int = 100, output_path: str = "raw data/flights_raw_data.json") -> None:
    """
    Fetches flight data from the Aviationstack API and saves it as a JSON file.

    Args:
        api_key (str): Your Aviationstack API key.
        limit (int): Number of records to fetch. Default is 100.
        output_path (str): File path where the fetched data will be stored.

    Raises:
        Exception: If the API request fails.
    """
    base_url = "https://api.aviationstack.com/v1/flights"
    params = {
        "access_key": api_key,
        "limit": limit,
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, "w") as file:
            json.dump(data, file, indent=4)

        print(f"Flight data saved to: {output_path}")
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code} | Response: {response.text}")

if __name__ == "__main__":
    API_KEY = "0465ee22c19ff1b147d3e525c1e81b49"
    try:
        fetch_flight_data(api_key=API_KEY)
    except Exception as e:
        print("Error:", e)
