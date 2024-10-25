import pandas as pd
import requests
import json
import io  # Import the StringIO module from io
import logging

# Function to fetch data from the SpotWx API
def fetch_spotwx_data(model, lat, lon, tz):
    logging.info(f"Fetching {model} data for lat: {lat}, lon: {lon}, tz: {tz}")

    api_key = ""  # Placeholder for the actual API key
    url = f"https://spotwx.io/api.php?key={api_key}&lat={lat}&lon={lon}&tz={tz}&model={model}"

    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data from {model} model for lat: {lat}, lon: {lon} - {e}")
        raise

    # Assuming the CSV is returned directly from the API
    if response.status_code == 200:
        data = pd.read_csv(io.StringIO(response.text))
        logging.info(f"Data fetched successfully from {model} model for lat: {lat}, lon: {lon}")

        # Add a model-specific DATETIME column to the data
        if 'DATETIME' in data.columns:
            data['DATETIME'] = pd.to_datetime(data['DATETIME'])
            logging.info(f"'DATETIME' column found and converted to datetime for {model} data")
        else:
            logging.error(f"'DATETIME' column not found in {model} data")
            raise Exception(f"'DATETIME' column not found in {model} data")

        return data
    else:
        logging.error(f"Failed to fetch data from {model} model with status code {response.status_code}")
        raise Exception(f"Failed to fetch data from {model} model for lat: {lat}, lon: {lon}")


# Function to filter fetched data according to parameters in JSON
def fetch_all_weather_data(site_code):
    logging.info(f"Fetching weather data for site: {site_code}")

    # Load JSON data
    json_file = "site_params.json"
    try:
        with open(json_file, 'r') as f:
            site_data = json.load(f)
        logging.info(f"Loaded JSON data from {json_file}")
    except Exception as e:
        logging.error(f"Failed to load JSON file: {json_file} - {e}")
        raise

    # Find the site from the JSON
    site_info = site_data["sites"].get(site_code)
    if not site_info:
        logging.error(f"Site {site_code} not found in JSON file.")
        raise Exception(f"Site {site_code} not found in JSON file.")

    lat = site_info["Lat"]
    lon = site_info["Lon"]
    tz = site_info["tz"]
    dst_observed = site_info["DSTobserved"]
    hubH = site_info["HubHeight"]

    # Adjust time zone for DST if necessary
    if dst_observed:
        tz += 1
        logging.info(f"DST observed for site {site_code}, adjusted timezone to {tz}")

    try:
        # Fetch HRRR, NAM, and GFS data
        hrrr_data = fetch_spotwx_data("hrrr", lat, lon, tz)
        nam_data = fetch_spotwx_data("nam", lat, lon, tz)
        gfs_data = fetch_spotwx_data("gfs", lat, lon, tz)
    except Exception as e:
        logging.error(f"Error fetching weather data for site {site_code}: {e}")
        raise

    # Get the specified weather parameters for HRRR, NAM, and GFS from the JSON
    hrrr_params = site_info["parameters"]["HRRR"]
    nam_params = site_info["parameters"]["NAM"]
    gfs_params = site_info["parameters"]["GFS"]

    # Filter dataframes to only include the specified parameters
    logging.info(f"Filtering HRRR, NAM, and GFS data for specified parameters in site {site_code}")
    hrrr_filtered = hrrr_data[['DATETIME'] + hrrr_params]
    nam_filtered = nam_data[['DATETIME'] + nam_params]
    gfs_filtered = gfs_data[['DATETIME'] + gfs_params]

    # Rename the columns to indicate which model the data comes from
    hrrr_filtered.columns = [f"HRRR_{col}" if col != 'DATETIME' else 'DATETIME' for col in hrrr_filtered.columns]
    nam_filtered.columns = [f"NAM_{col}" if col != 'DATETIME' else 'DATETIME' for col in nam_filtered.columns]
    gfs_filtered.columns = [f"GFS_{col}" if col != 'DATETIME' else 'DATETIME' for col in gfs_filtered.columns]
    logging.info("Renamed columns to reflect model names")

    # Convert Wind Speed from km/h to m/s for all columns containing "WSPD"
    logging.info("Converting wind speed from km/h to m/s")
    for col in hrrr_filtered.columns:
        if "WSPD" in col:
            hrrr_filtered[col] = hrrr_filtered[col] * 0.27778  # Convert from km/h to m/s
            hrrr_filtered[col] = hrrr_filtered[col] * (hubH / 80) ** 0.2  # wind speed correction for Hub height

    for col in nam_filtered.columns:
        if "WSPD" in col:
            nam_filtered[col] = nam_filtered[col] * 0.27778  # Convert from km/h to m/s
            nam_filtered[col] = nam_filtered[col] * (hubH / 80) ** 0.2  # wind speed correction for Hub height

    for col in gfs_filtered.columns:
        if "WSPD" in col:
            gfs_filtered[col] = gfs_filtered[col] * 0.27778  # Convert from km/h to m/s
            gfs_filtered[col] = gfs_filtered[col] * (hubH / 80) ** 0.2  # wind speed correction for Hub height

    # Merge the dataframes based on 'DATETIME', using an outer join to handle different update rates
    logging.info("Merging filtered data from HRRR, NAM, and GFS models")
    merged_df = pd.merge(hrrr_filtered, nam_filtered, on='DATETIME', how='outer')
    final_df = pd.merge(merged_df, gfs_filtered, on='DATETIME', how='outer')

    logging.info(f"Weather data for site {site_code} successfully processed")
    return final_df, hrrr_data, nam_data, gfs_data



