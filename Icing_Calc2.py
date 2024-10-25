import pandas as pd
import numpy as np
import logging
import json


def site_spec(site):
    # Load JSON for site RH thresholds
    json_file = "site_params.json"
    try:
        with open(json_file, 'r') as f:
            site_data = json.load(f)
        logging.info(f"Loaded JSON data from {json_file}")
    except Exception as e:
        logging.error(f"Failed to load JSON file: {json_file} - {e}")
        raise

    site_specs = site_data["sites"].get(site)
    if not site_specs:
        logging.error(f"Site {site} not found in JSON file.")
        raise Exception(f"Site {site} not found in JSON file.")

    return site_specs



def rename_columns(final_data):
    # Define a mapping for renaming columns based on substrings found
    rename_map = {
        'WSPD': 'FCST_WS',
        'TMP': 'FCST_Temp',
        'RH': 'FCST_RH',
        'SQP': 'FCST_SnowWater'
    }

    # Define the possible prefixes
    prefixes = ['HRRR_', 'NAM_', 'GFS_']

    # Iterate over the columns in the DataFrame
    for col in final_data.columns:
        for old_value, new_value in rename_map.items():
            # Check if the column name contains the prefix and the substring
            if any(col.startswith(prefix) for prefix in prefixes) and old_value in col:
                # Rename by replacing the entire column name with 'FCST-' + the new value
                final_data.rename(columns={col: new_value}, inplace=True)

    return final_data


def calculate_glaze_hardrime_icing(final_data, site):

    def Curve_IceType_calculations(group, ws_col, temp_col, snow_water_col, rh_col):
        # Calculate Glaze and Hard Rime for the group
        final_data[f'{group}_GlazeCurve'] = final_data.apply(
            lambda row: (-0.0004 * row[ws_col] ** 3) + (0.0215 * row[ws_col] ** 2) - (
                    0.6266 * row[ws_col]) + 1.4396
            if not np.isnan(row[ws_col]) else np.nan, axis=1
        )

        final_data[f'{group}_HardRimeCurve'] = final_data.apply(
            lambda row: (-0.0014 * row[ws_col] ** 3) + (0.0705 * row[ws_col] ** 2) - (
                    1.5798 * row[ws_col]) + 1.3887
            if not np.isnan(row[ws_col]) else np.nan, axis=1
        )

        # IcingTypeZone calculation
        final_data[f'{group}_IcingTypeZone'] = final_data.apply(
            lambda row: "NO ICE" if row[temp_col] >= 0 else (
                "Glaze" if row[temp_col] > row[f'{group}_GlazeCurve'] else (
                    "Hard Rime" if row[temp_col] <= row[f'{group}_GlazeCurve'] and row[temp_col] > row[
                        f'{group}_HardRimeCurve'] else "Soft Rime"
                )
            ) if not np.isnan(row[temp_col]) and not np.isnan(row[f'{group}_GlazeCurve']) and not np.isnan
                (row[f'{group}_HardRimeCurve'])
            else np.nan, axis=1
        )

    def Zone_Icing_calculations(group, ws_col, temp_col, snow_water_col, rh_col,IceSev_col):

        if "MCMS" in group:
            final_data[f'{group}_Icing'] = final_data.apply(
                lambda row: row[f'{group}_IcingTypeZone'] if row[IceSev_col] > 0 or row
                [snow_water_col] > 0
                else "NO ICE"
                if not np.isnan(row[IceSev_col]) and not np.isnan(row[snow_water_col])
                   and not pd.isna(row[f'{group}_IcingTypeZone']) else np.nan, axis=1
            )
        else:
            final_data[f'{group}_Icing'] = final_data.apply(
                lambda row: row[f'{group}_IcingTypeZone'] if row[snow_water_col] > 1 or row
                [rh_col] > row[f'{group}_RH_Threshold']
                else "NO ICE"
                if not np.isnan(row[snow_water_col]) and not np.isnan
                (row[rh_col]) and not np.isnan(row[f'{group}_RH_Threshold'])
                   and not pd.isna(row[f'{group}_IcingTypeZone']) else np.nan, axis=1
        )

        # Additional calculations (WindZone, PitchZone, TempZone)
        final_data[f'{group}_WindZone m/s'] = final_data[ws_col].apply(
            lambda ws: WZ["zone1"][0] if ws <= WZ["zone1"][1] else (WZ["zone2"][0] if ws <= WZ["zone2"][1] else (WZ["zone3"][0] if ws <= WZ["zone3"][1] else WZ["zone4"][0]))
            if not np.isnan(ws) else np.nan
        )

        PitchIN_ws = site_info["Pitch"]["PitchIN_ws"]
        PitchOUT_ws = site_info["Pitch"]["PitchOUT_ws"]

        final_data[f'{group}_PitchZone'] = final_data[ws_col].apply(
            lambda ws: "Pitch In" if ws <= PitchIN_ws else ("Pitch Stay" if ws <= PitchOUT_ws else "Pitch Out")
            if not np.isnan(ws) else np.nan
        )

        final_data[f'{group}_TempZone'] = final_data[temp_col].apply(
            lambda temp: (
                "Below -15" if temp < -15 else
                "-10 to -15" if -15 <= temp < -10 else
                "-5 to -10" if -10 <= temp < -5 else
                "0 to -5" if -5 <= temp < 0 else
                "Above 0"
            ) if not np.isnan(temp) else np.nan
        )

    # Handle different groups dynamically
    groups = {
        'FCST': ['FCST_WS', 'FCST_Temp', 'FCST_SnowWater', 'FCST_RH', 'FCST_RH'],
        'MCMS': [f'{site}-MCMS-WindSpeed', f'{site}-MCMS-NacelleTemperature2', f'{site}-MCMS-LWC', f'{site}-MCMS-RH', f'{site}-MCMS-IcingSeverity'],
        'MCMS1': [f'{site}-MCMS1-WindSpeed', f'{site}-MCMS1-NacelleTemperature2', f'{site}-MCMS1-LWC', f'{site}-MCMS1-RH', f'{site}-MCMS1-IcingSeverity'],
        'MCMS2': [f'{site}-MCMS2-WindSpeed', f'{site}-MCMS2-NacelleTemperature2', f'{site}-MCMS2-LWC', f'{site}-MCMS2-RH', f'{site}-MCMS2-IcingSeverity']
    }

    # Loop over groups for Curves and Icetype
    for group, columns in groups.items():
        if all(col in final_data.columns for col in columns):
            Curve_IceType_calculations(group, columns[0], columns[1], columns[2], columns[3])


    site_info = site_spec(site)

    RH_Glaze = site_info["RHTreshold"]["RHGlaze"]
    RH_HardR = site_info["RHTreshold"]["RHHardR"]
    RH_SoftR = site_info["RHTreshold"]["RHSoftR"]

    WZ = site_info["WindZone"]

    # Apply RH threshold and icing calculation to all groups
    for group in groups.keys():
        if f'{group}_IcingTypeZone' in final_data.columns:
            final_data[f'{group}_RH_Threshold'] = final_data.apply(
                lambda row: RH_Glaze if row[f'{group}_IcingTypeZone'] == "Glaze" else (
                    RH_HardR if row[f'{group}_IcingTypeZone'] == "Hard Rime" else (
                        RH_SoftR if row[f'{group}_IcingTypeZone'] == "Soft Rime" else 0
                    )
                ) if not pd.isna(row[f'{group}_IcingTypeZone']) else np.nan, axis=1
            )

    # Loop over groups for Zones and Icing
    for group, columns in groups.items():
        if all(col in final_data.columns for col in columns):
            if "MCMS" in group:
                Zone_Icing_calculations(group, columns[0], columns[1], columns[2], columns[3], columns[4])
            else:
                Zone_Icing_calculations(group, columns[0], columns[1], columns[2], columns[3], columns[4])

    return final_data




def process_final_data(final_data , site):
    final_data = rename_columns(final_data)
    calculate_glaze_hardrime_icing(final_data, site)

    return final_data
