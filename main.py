import pandas as pd
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from SpotWx_fetch import fetch_all_weather_data
from turbine_sensor_fetch import turbine_1h_data, MCMS_get_data
import excel_handler2  # Import the new excel handling module
from Icing_Calc2 import process_final_data
from icing_check_email import check_icing_condition
from clean_format_combine import combine_data, reorder_columns

# Setup logging to log both to file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler('process_log.log'),
        logging.StreamHandler()
    ]
)


def process_site(site):
    """Process data for a single site."""
    try:
        logging.info(f"Starting data fetch and merge process for site: {site}")

        # Fetch weather data from SpotWx
        weather_data, hrrr_data, nam_data, gfs_data = fetch_all_weather_data(site)

        # Fetch turbine production data and sensor data (MCMS)
        turbines_data = turbine_1h_data(site)
        MCMS_data = MCMS_get_data(site)

        # Combine turbine, sensor, and weather data
        combined_data = combine_data(turbines_data, MCMS_data, weather_data)

        # Process the final_data for calculations and check for icing conditions
        final_data = process_final_data(combined_data, site)
        check_icing_condition(final_data, site)

        # Define the Excel file path
        # existing_file = f'{site}_weather_site_data.xlsx'
        file_name = f'{site}_weather_site_data.xlsx'
        existing_file = os.path.join(
            "C:\\Users\\aebrahimi\\OneDrive - Liberty\\General\\2.Programs\\Icing\\ICING STRATEGY 2023\\test",
            file_name)

        # Update all sheets with the new data
        final_data, hrrr_data, nam_data, gfs_data = excel_handler2.update_all_sheets(existing_file, combined_data,hrrr_data, nam_data, gfs_data)

        final_data = reorder_columns(final_data, site)

        # Export the updated data back to the Excel file
        excel_handler2.export_to_excel(final_data, hrrr_data, nam_data, gfs_data, site)

        logging.info(f"Data process completed successfully for site: {site}")

    except Exception as e:
        logging.error(f"Error occurred during the process for site {site}: {e}", exc_info=True)

def main():
    # Site list to fetch data for each site
    site_list = ['SENT', 'SNDY','MN', 'MAV', 'SUGR', 'DFS', 'OWF', 'SO', 'SL', 'RLWEP', 'MOR', 'AMHST', 'BLH'] #

    # Use ThreadPoolExecutor to run all sites concurrently
    with ThreadPoolExecutor(max_workers=len(site_list)) as executor:
        futures = {executor.submit(process_site, site): site for site in site_list}

        for future in as_completed(futures):
            site = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing site {site}: {e}", exc_info=True)


if __name__ == "__main__":
    main()
