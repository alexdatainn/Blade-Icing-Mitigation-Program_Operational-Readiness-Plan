import pandas as pd
from datetime import datetime, timedelta
import json
import win32com.client  # For sending email


def get_site_current_time(site_name):
    """Get the current time for the site, adjusting for UTC offset and DST."""
    with open('site_params.json', 'r') as f:
        site_data = json.load(f)

    site_info = site_data["sites"].get(site_name)
    if not site_info:
        raise ValueError(f"Site {site_name} not found in the JSON file.")

    tz_offset = site_info["tz"]
    dst_observed = site_info["DSTobserved"]

    # Adjust time zone for DST if necessary
    current_utc_time = datetime.utcnow()
    if dst_observed:
        tz_offset += 1

    site_time = current_utc_time + timedelta(hours=tz_offset)
    return site_time

def check_icing_condition(final_data, site_name):
    # Cooldown JSON file path
    cooldown_file = 'cooldown_data.json'

    # Define icing types and filter data for icing conditions
    icing_conditions = ['Glaze', 'Hard Rime', 'Soft Rime']

    # Get the current time for the site
    current_time = current_time = get_site_current_time(site_name)
    future_time = current_time + timedelta(hours=12)
    # Filter final_data for the current site and the next 12 hours
    site_data = final_data[(final_data['DATETIME'] >= current_time) & (final_data['DATETIME'] <= future_time)]

    # Check if any icing condition is met for 3 consecutive times within the next 12 hours
    site_data['FCST_Icing'] = site_data['FCST_Icing'].fillna('NO ICE')  # Handle missing values if needed
    icing_flag = site_data['FCST_Icing'].isin(icing_conditions).astype(int)

    # Find consecutive 1s in the icing_flag
    consecutive_count = icing_flag.rolling(window=3).sum()

    if (consecutive_count >= 3).any():
        # Load the JSON file to check for previous alerts
        try:
            with open(cooldown_file, 'r') as f:
                cooldown_data = json.load(f)
        except FileNotFoundError:
            cooldown_data = {}

        # Check if an alert was sent within the last 6 hours
        if site_name in cooldown_data:
            last_alert_time = datetime.strptime(cooldown_data[site_name], '%Y-%m-%d %H:%M:%S')
            if current_time < last_alert_time + timedelta(hours=6):
                print(f"Cooldown active for {site_name}. No email will be sent.")
                return

        # Trigger email and log alert
        send_email(site_name)

        # Update JSON with the current alert timestamp
        cooldown_data[site_name] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        with open(cooldown_file, 'w') as f:
            json.dump(cooldown_data, f)


def send_email(site_name):
    # Replace the placeholders with your email configuration
    outlook = win32com.client.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)

    mail.Subject = f'Consecutive Icing Signs - {site_name}'
    mail.Body = f'The condition of three hours consecutive of icing possibility in the next 12 hours has been met at {site_name}.'

    contact_list = ['alex.ebrahimi@algonquinpower.com']
    mail.To = "; ".join(contact_list)
    mail.Send()

