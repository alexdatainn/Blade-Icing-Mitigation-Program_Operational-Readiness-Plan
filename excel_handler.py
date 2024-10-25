import pandas as pd
import os
import logging
import xlsxwriter  # Import xlsxwriter for utility functions

# # Create a function to export data to an Excel file
# def export_to_excel(final_data, hrrr_data, nam_data, gfs_data, site_name):
#     file_name = f'{site_name}_weather_site_data.xlsx'
#     logging.info(f"Exporting data to Excel file: {file_name}")
#
#     with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
#         # Write the combined data to the first sheet
#         final_data.round(1).to_excel(writer, sheet_name='Combined_Data', index=False)
#
#         # Write each weather model's data to its own sheet
#         hrrr_data.to_excel(writer, sheet_name='HRRR', index=False)
#         nam_data.to_excel(writer, sheet_name='NAM', index=False)
#         gfs_data.to_excel(writer, sheet_name='GFS', index=False)
#
#     logging.info(f"Data successfully exported to {file_name}")


import pandas as pd
import logging

def apply_conditional_formatting(workbook, worksheet, df, icing_column_name='Icing'):
    """
    Applies conditional formatting to the 'Icing' column in the worksheet.

    Parameters:
    workbook (xlsxwriter.Workbook): The workbook object from ExcelWriter.
    worksheet (xlsxwriter.Worksheet): The worksheet where data is written.
    df (pandas.DataFrame): The dataframe containing the data.
    icing_column_name (str): The name of the column with icing data (default 'Icing').
    """
    # Check if the icing column exists
    if icing_column_name not in df.columns:
        logging.warning(f"Column '{icing_column_name}' not found in DataFrame.")
        return

    # Get the index of the icing column
    icing_col_idx = df.columns.get_loc(icing_column_name)  # 0-based index

    # Convert the index to Excel-style column letter
    icing_col_letter = xlsxwriter.utility.xl_col_to_name(icing_col_idx)

    # Define the range of the Icing column (e.g., B2:B100)
    icing_range = f'{icing_col_letter}2:{icing_col_letter}{len(df) + 1}'

    # Define the formatting styles for the different icing conditions
    glaze_format = workbook.add_format({'bg_color': '#0000FF'})  #  Blue
    hard_rime_format = workbook.add_format({'bg_color': '#FFFF00'})  # Yellow
    soft_rime_format = workbook.add_format({'bg_color': '#FF0000'})  # Red

    # Apply conditional formatting for each icing condition
    worksheet.conditional_format(icing_range, {'type': 'text',
                                               'criteria': 'containing',
                                               'value': 'Glaze',
                                               'format': glaze_format})

    worksheet.conditional_format(icing_range, {'type': 'text',
                                               'criteria': 'containing',
                                               'value': 'Hard Rime',
                                               'format': hard_rime_format})

    worksheet.conditional_format(icing_range, {'type': 'text',
                                               'criteria': 'containing',
                                               'value': 'Soft Rime',
                                               'format': soft_rime_format})

    logging.info(f"Conditional formatting applied to '{icing_column_name}' column.")



def export_to_excel(final_data, hrrr_data, nam_data, gfs_data, site_name):
    file_name = f'{site_name}_weather_site_data.xlsx'
    logging.info(f"Exporting data to Excel file: {file_name}")

    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        # Write the combined data to the first sheet
        final_data.round(1).to_excel(writer, sheet_name='Combined_Data', index=False)

        # Write each weather model's data to its own sheet
        hrrr_data.to_excel(writer, sheet_name='HRRR', index=False)
        nam_data.to_excel(writer, sheet_name='NAM', index=False)
        gfs_data.to_excel(writer, sheet_name='GFS', index=False)

        # Access the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet_combined = writer.sheets['Combined_Data']

        # Apply conditional formatting on 'Combined_Data' sheet
        apply_conditional_formatting(workbook, worksheet_combined, final_data, icing_column_name='Icing')

    logging.info(f"Data successfully exported to {file_name}")



#
# def update_existing_data(existing_data, new_data):
#     logging.info("Updating existing data with new weather updates")
#
#     # Find the earliest time in the new data
#     new_start_time = new_data['DATETIME'].min()
#
#     # Filter out rows in existing data where DATETIME is equal or after the new_start_time
#     existing_data_filtered = existing_data[existing_data['DATETIME'] < new_start_time]
#
#     # Combine the filtered existing data with the new data
#     updated_data = pd.concat([existing_data_filtered, new_data], ignore_index=True)
#
#     return updated_data


def update_existing_data(existing_data, new_data):
    """
    only valid (non-null) new data replaces the existing data, and new rows from new_data are appended if they don’t exist in existing_data
    """
    logging.info("Updating existing data with new weather updates")

    # Set 'DATETIME' column as index for easier updating but keep 'DATETIME' as a column
    existing_data.set_index('DATETIME', inplace=True, drop=False)
    new_data.set_index('DATETIME', inplace=True, drop=False)

    # Iterate over the new_data rows and update the corresponding existing_data rows
    for datetime, new_row in new_data.iterrows():
        if datetime in existing_data.index:
            # Update existing data only for non-null values in new_data
            for column in new_data.columns:
                if pd.notna(new_row[column]):  # Only update if new_row value is not NaN
                    existing_data.at[datetime, column] = new_row[column]
        else:
            # If the datetime doesn't exist in existing_data, append the new row
            existing_data = pd.concat([existing_data, pd.DataFrame([new_row], index=[datetime])])

    # Reset index back to default without renaming 'DATETIME'
    existing_data.reset_index(drop=True, inplace=True)

    return existing_data


def update_all_sheets(existing_file, new_combined_data, hrrr_data, nam_data, gfs_data):
    # Load existing data from all sheets
    if os.path.exists(existing_file):
        logging.info(f"Reading existing data from {existing_file}")
        existing_data = pd.read_excel(existing_file, sheet_name='Combined_Data')
        existing_hrrr = pd.read_excel(existing_file, sheet_name='HRRR')
        existing_nam = pd.read_excel(existing_file, sheet_name='NAM')
        existing_gfs = pd.read_excel(existing_file, sheet_name='GFS')

        # Update each sheet's data
        final_data = update_existing_data(existing_data, new_combined_data)
        hrrr_data = update_existing_data(existing_hrrr, hrrr_data)
        nam_data = update_existing_data(existing_nam, nam_data)
        gfs_data = update_existing_data(existing_gfs, gfs_data)
    else:
        logging.info(f"No existing file found. Using new data.")
        final_data = new_combined_data

    return final_data, hrrr_data, nam_data, gfs_data
