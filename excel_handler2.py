import pandas as pd
import os
import logging
import time
import xlsxwriter


def is_file_locked(filepath):
    """
    Check if a file is locked (in use) by trying to open it exclusively.
    If the file can be opened, it's not locked.
    """
    if not os.path.exists(filepath):
        return False  # File doesn't exist, so it's not locked

    try:
        with open(filepath, 'a'):  # Try to open the file for appending
            pass
    except IOError:
        return True  # The file is locked

    return False


def wait_for_file(filepath, retries=50, wait_time=60):
    """
    Wait for the file to be available if it's locked, and retry after a delay.
    """
    retry_count = 0
    while is_file_locked(filepath):
        if retry_count >= retries:
            logging.error(f"File {filepath} is locked after {retries} retries. Giving up.")
            raise IOError(f"File {filepath} is locked and can't be accessed.")

        logging.warning(f"File {filepath} is locked. Retrying in {wait_time} seconds...")
        time.sleep(wait_time)
        retry_count += 1
    logging.info(f"File {filepath} is now accessible.")


def apply_conditional_formatting(workbook, worksheet, df, icing_column_name):
    """
    Applies conditional formatting to the 'Icing' column in the worksheet.
    """
    for IcingCol in icing_column_name:
        if IcingCol not in df.columns:
            logging.warning(f"Column '{IcingCol}' not found in DataFrame.")
            continue  # Skip to the next iteration if column is not found

        icing_col_idx = df.columns.get_loc(IcingCol)  # 0-based index
        icing_col_letter = xlsxwriter.utility.xl_col_to_name(icing_col_idx)
        icing_range = f'{icing_col_letter}2:{icing_col_letter}{len(df) + 1}'

        # Define formatting styles for icing conditions
        glaze_format = workbook.add_format({'bg_color': '#0000FF'})  # Blue
        hard_rime_format = workbook.add_format({'bg_color': '#FFFF00'})  # Yellow
        soft_rime_format = workbook.add_format({'bg_color': '#FF0000'})  # Red

        # Apply conditional formatting for each icing condition
        worksheet.conditional_format(icing_range, {'type': 'text', 'criteria': 'containing', 'value': 'Glaze',
                                                   'format': glaze_format})
        worksheet.conditional_format(icing_range, {'type': 'text', 'criteria': 'containing', 'value': 'Hard Rime',
                                                   'format': hard_rime_format})
        worksheet.conditional_format(icing_range, {'type': 'text', 'criteria': 'containing', 'value': 'Soft Rime',
                                                   'format': soft_rime_format})

        logging.info(f"Conditional formatting applied to '{IcingCol}' column.")


def export_to_excel(final_data, hrrr_data, nam_data, gfs_data, site_name):
    """
    Export the final combined weather and turbine data to an Excel file, with conditional formatting.
    """
    file_name = f'{site_name}_weather_site_data.xlsx'
    file_path = os.path.join(
        "C:\\Users\\aebrahimi\\OneDrive - Liberty\\General\\2.Programs\\Icing\\ICING STRATEGY 2023\\test", file_name)

    logging.info(f"Exporting data to Excel file: {file_name}")

    # Wait for the file to be unlocked
    wait_for_file(file_path)

    # Write data to Excel
    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        final_data.round(1).to_excel(writer, sheet_name='Combined_Data', index=False)
        hrrr_data.to_excel(writer, sheet_name='HRRR', index=False)
        nam_data.to_excel(writer, sheet_name='NAM', index=False)
        gfs_data.to_excel(writer, sheet_name='GFS', index=False)

        # Access workbook and worksheet to apply formatting
        workbook = writer.book
        worksheet_combined = writer.sheets['Combined_Data']
        apply_conditional_formatting(workbook, worksheet_combined, final_data,
                                     icing_column_name=['FCST_Icing', 'MCMS_Icing', 'MCMS1_Icing', 'MCMS2_Icing'])

    logging.info(f"Data successfully exported to {file_path}")


def fill_missing_hours(existing_data, new_data):
    """
    Ensure both existing_data and new_data have a complete set of hourly DATETIME values,
    filling in missing hours with NaN for all other columns.
    """
    logging.info("Filling missing hourly intervals in DATETIME.")

    # Normalize the DATETIME columns to ensure they are exactly on the hour
    existing_data['DATETIME'] = pd.to_datetime(existing_data['DATETIME']).dt.floor('H')
    new_data['DATETIME'] = pd.to_datetime(new_data['DATETIME']).dt.floor('H')

    # Combine both existing and new DATETIME columns to find the full hourly range
    combined_datetime = pd.date_range(
        start=min(existing_data['DATETIME'].min(), new_data['DATETIME'].min()),
        end=max(existing_data['DATETIME'].max(), new_data['DATETIME'].max()),
        freq='H'  # Frequency of 1 hour
    )

    # Set the 'DATETIME' column as index temporarily
    existing_data.set_index('DATETIME', inplace=True, drop=False)
    new_data.set_index('DATETIME', inplace=True, drop=False)

    # Find the missing hours in both existing_data and new_data
    missing_in_existing = combined_datetime.difference(existing_data.index)
    missing_in_new = combined_datetime.difference(new_data.index)

    # Create empty DataFrames for the missing hours, filled with NaN
    missing_existing_df = pd.DataFrame(index=missing_in_existing, columns=existing_data.columns).reset_index()
    missing_existing_df['DATETIME'] = missing_existing_df['index']
    missing_existing_df.drop(columns=['index'], inplace=True)
    missing_existing_df.iloc[:, 1:] = pd.NA  # Fill all other columns with NaN

    missing_new_df = pd.DataFrame(index=missing_in_new, columns=new_data.columns).reset_index()
    missing_new_df['DATETIME'] = missing_new_df['index']
    missing_new_df.drop(columns=['index'], inplace=True)
    missing_new_df.iloc[:, 1:] = pd.NA  # Fill all other columns with NaN

    # Concatenate missing hours with the original data
    existing_data = pd.concat([existing_data, missing_existing_df], ignore_index=True).sort_values('DATETIME')
    new_data = pd.concat([new_data, missing_new_df], ignore_index=True).sort_values('DATETIME')

    # Reset index and ensure 'DATETIME' remains a column
    existing_data.reset_index(drop=True, inplace=True)
    new_data.reset_index(drop=True, inplace=True)

    return existing_data, new_data

def update_existing_data(existing_data, new_data):
    """
    Update existing data with new weather updates, replacing only valid (non-null) new data.
    Append new rows from new_data if they don’t exist in existing_data.
    """
    logging.info("Updating existing data with new weather updates")

    existing_data.set_index('DATETIME', inplace=True, drop=False)
    new_data.set_index('DATETIME', inplace=True, drop=False)

    for datetime, new_row in new_data.iterrows():
        if datetime in existing_data.index:
            for column in new_data.columns:
                if pd.notna(new_row[column]):
                    existing_data.at[datetime, column] = new_row[column]
        else:
            existing_data = pd.concat([existing_data, pd.DataFrame([new_row], index=[datetime])])

    existing_data.reset_index(drop=True, inplace=True)
    return existing_data


def update_all_sheets(existing_file, new_combined_data, hrrr_data, nam_data, gfs_data):
    """
    Update existing Excel sheets with new data for Combined_Data, HRRR, NAM, and GFS sheets,
    ensuring all hourly intervals are filled.
    """
    if os.path.exists(existing_file):
        # Wait for the file to be unlocked
        wait_for_file(existing_file)

        logging.info(f"Reading existing data from {existing_file}")
        existing_data = pd.read_excel(existing_file, sheet_name='Combined_Data')
        existing_hrrr = pd.read_excel(existing_file, sheet_name='HRRR')
        existing_nam = pd.read_excel(existing_file, sheet_name='NAM')
        existing_gfs = pd.read_excel(existing_file, sheet_name='GFS')

        # Fill missing hourly intervals before updating data
        existing_data, new_combined_data = fill_missing_hours(existing_data, new_combined_data)
        final_data = update_existing_data(existing_data, new_combined_data)

        existing_hrrr, hrrr_data = fill_missing_hours(existing_hrrr, hrrr_data)
        hrrr_data = update_existing_data(existing_hrrr, hrrr_data)

        existing_nam, nam_data = fill_missing_hours(existing_nam, nam_data)
        nam_data = update_existing_data(existing_nam, nam_data)

        existing_gfs, gfs_data = fill_missing_hours(existing_gfs, gfs_data)
        gfs_data = update_existing_data(existing_gfs, gfs_data)
    else:
        logging.info(f"No existing file found. Using new data.")
        final_data = new_combined_data

    return final_data, hrrr_data, nam_data, gfs_data
