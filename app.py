from flask import Flask, jsonify
import pandas as pd
import hashlib
from loguru import logger
import os
from pathlib import Path
import csv
import tabulate
import FileSelection

app = Flask(__name__)

def find_folder(folder_name):
    for drive in ["C:/", "D:/"]:
        for root, dirs, files in os.walk(drive):
            if folder_name in dirs:
                return os.path.join(root, folder_name)
    return None

def remove_log_file(log_file_path):
    try:
        os.remove(log_file_path)
        print(f"Log file '{log_file_path}' removed successfully.")
    except FileNotFoundError:
        print(f"Error: Log file '{log_file_path}' not found.")
    except PermissionError:
        print(f"Error: Permission denied to delete '{log_file_path}'.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def configure_logger(log_file):
    logger.add(log_file, level="INFO")

def hash_row(row):
    row_string = ','.join(map(str, row))
    return hashlib.md5(row_string.encode()).hexdigest()

def load_csv(file_path):
    return pd.read_csv(file_path)

def add_hash_column(df):
    df['hash'] = df.apply(hash_row, axis=1)
    return df

def get_differences(today_df, yesterday_df):
    today_dict = dict(zip(today_df['Employee ID'], today_df['hash']))
    yesterday_dict = dict(zip(yesterday_df['Employee ID'], yesterday_df['hash']))
    
    new_entries = [emp_id for emp_id in today_dict if emp_id not in yesterday_dict]
    removed_entries = [emp_id for emp_id in yesterday_dict if emp_id not in today_dict]
    modified_entries = [emp_id for emp_id in today_dict if emp_id in yesterday_dict and today_dict[emp_id] != yesterday_dict[emp_id]]

    logger.info(f"Modified entries reports generated successfully! Count: {len(modified_entries)}")
    logger.info(f"Removed entries reports generated successfully! Count: {len(removed_entries)}")
    logger.info(f"New entries reports generated successfully! Count: {len(new_entries)}")
    
    return new_entries, removed_entries, modified_entries

@app.route('/compare', methods=['GET'])
def compare_hr_data():
    folder_path = find_folder("files_in")
    if not folder_path:
        return jsonify({"error": "The folder 'files_in' was not found in the system."}), 404

    recent_files = FileSelection.get_recent_files(folder_path, 2, keyword="UTA_HR_MAVEXPRESS_EXPORT")
    if len(recent_files) < 2:
        return jsonify({"error": "Not enough files found to compare."}), 400

    today_file = recent_files[0]
    yesterday_file = recent_files[1]

    base_dir = Path(__file__).resolve().parent
    log_file = base_dir / "hr_details.log"
    output_dir = base_dir / "output"
    os.makedirs(output_dir, exist_ok=True)

    remove_log_file(log_file)
    configure_logger(log_file)

    df_today = add_hash_column(load_csv(today_file))
    df_yesterday = add_hash_column(load_csv(yesterday_file))

    new_entries, removed_entries, modified_entries = get_differences(df_today, df_yesterday)

    return jsonify({
        "new_entries": new_entries,
        "removed_entries": removed_entries,
        "modified_entries": modified_entries
    })

if __name__ == "__main__":
    app.run(debug=True)