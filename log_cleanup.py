import os
from datetime import datetime, timedelta


def delete_old_logs(log_file_path, days_to_keep=3):
    """
    Delete log entries older than 'days_to_keep' days from the log file.

    Parameters:
    log_file_path (str): The path to the log file.
    days_to_keep (int): The number of days' worth of logs to keep.
    """
    # Calculate the cutoff datetime
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)

    # Check if the log file exists
    if not os.path.exists(log_file_path):
        print(f"Log file {log_file_path} does not exist.")
        return

    # Read the log file
    with open(log_file_path, 'r') as log_file:
        lines = log_file.readlines()

    # Open the log file in write mode
    with open(log_file_path, 'w') as log_file:
        for line in lines:
            # Extract the date from the log entry (assumed format: YYYY-MM-DD HH:MM:SS)
            log_datetime_str = line.split(' - ')[0]
            try:
                log_datetime = datetime.strptime(log_datetime_str, "%Y-%m-%d %H:%M:%S,%f")
                # Write the line back if it is within the allowed date range
                if log_datetime >= cutoff_date:
                    log_file.write(line)
            except ValueError:
                # In case the log line does not match the expected format, keep it
                log_file.write(line)


if __name__ == "__main__":
    # Path to the process_log.log file
    log_file_path = "process_log.log"  # Replace with your actual log file path
    delete_old_logs(log_file_path)
