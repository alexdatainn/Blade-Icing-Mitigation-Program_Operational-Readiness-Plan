import pandas as pd
import logging




def reorder_columns(df, site):
    """
    Bring specified columns to the front and place columns containing 'MCMS' after them.

    Parameters:
    df (pd.DataFrame): The original dataframe.
    first_columns (list): List of columns to bring to the front.
    mcms_keyword (str): The keyword to identify the 'MCMS' columns.

    Returns:
    pd.DataFrame: Dataframe with the specified columns and 'MCMS' columns in the desired order.
    """
    logging.info("Bring specified columns to the front and then MCMS")

    # first_columns = ['DATETIME', 'Icing', 'WindZone',  'TempZone','PitchZone',
    #                       'FCST_WS', 'FCST_Temp', 'FCST_RH', 'FCST_SnowWater',
    #                       'GlazeCurve', 'HardRimeCurve', 'IcingTypeZone', 'RH_Threshold']

    #order productions
    PI_keyword = "PerformanceIndex"
    AP_keyword = "ActivePower"
    TP_keyword = "TheoreticalPower"

    # Ensure that the specified first columns exist in the DataFrame
    first_columns = ['DATETIME']

    fcst_cols = ['FCST_Icing'] + [col for col in df.columns if 'FCST' in col and col != 'FCST_Icing']

    # List of columns that contain 'MCMS' (but not 'MCMS1' or "MCMS2") and only select calculated MCMS cols
    mcms_to_remove = [col for col in df.columns if f'{site}-MCMS' in col]
    mcms_cols = [col for col in df.columns if 'MCMS' in col and 'MCMS1' not in col and 'MCMS2' not in col and col not in mcms_to_remove]

    mcms1_cols = [col for col in df.columns if 'MCMS1' in col and col not in mcms_to_remove]

    mcms2_cols = [col for col in df.columns if 'MCMS2' in col and col not in mcms_to_remove]

    # Get all columns containing productions
    PI_columns = [col for col in df.columns if PI_keyword in col]
    AP_columns = [col for col in df.columns if AP_keyword in col]
    TP_columns = [col for col in df.columns if TP_keyword in col]

    # Get the remaining columns that are neither in the first columns nor in the MCMS columns
    remaining_columns = [col for col in df.columns if col not in first_columns + fcst_cols + mcms_cols + mcms1_cols + mcms2_cols + PI_columns + AP_columns + TP_columns]

    # Create a new column order: first_columns + mcms_columns + remaining_columns
    new_order = first_columns + fcst_cols + mcms_cols + mcms1_cols + mcms2_cols + remaining_columns + PI_columns + AP_columns + TP_columns

    # Reorder the DataFrame columns
    return df[new_order]


def clean_and_format_data(merged_data):
    logging.info("Cleaning and formatting the combined data")

    # Ensure the data starts from where the turbine/sensor data starts
    start_time = merged_data['DateTimeLocal'].min()
    final_data = merged_data[merged_data['DATETIME'] >= start_time]

    # Identify columns that contain 'DateTime' or 'datetime' (case-insensitive) except 'DATETIME'
    datetime_cols = [col for col in final_data.columns if 'datetime' in col.lower() and col != 'DATETIME']

    # Drop all datetime-related columns except 'DATETIME'
    final_data = final_data.drop(columns=datetime_cols, errors='ignore')

    # Move 'DATETIME' to the first column if it exists
    if 'DATETIME' in final_data.columns:
        final_data = final_data[['DATETIME'] + [col for col in final_data.columns if col != 'DATETIME']]




    return final_data

def combine_data(turbines_data, MCMS_data, weather_data):
    logging.info("Combining turbine, sensor, and weather data")

    # Merge turbines_data and MCMS_data on the same datetime column
    merged_data = pd.merge(turbines_data, MCMS_data, on="DateTimeLocal", how="inner")

    # Merge with weather data on the same datetime column
    final_data = pd.merge(merged_data, weather_data, left_on="DateTimeLocal", right_on="DATETIME", how="right")

    # Clean and format the combined data
    final_data = clean_and_format_data(final_data)

    return final_data


