import pandas as pd
import hashlib
from loguru import logger
import os
from pathlib import Path
import csv
import tabulate
import FileSelection

# Define the folder path where the files are located
# Search for the folder named 'files_in' in the system
def find_folder(folder_name):
    for drive in ["C:/", "D:/"]:
        for root, dirs, files in os.walk(drive):
            if folder_name in dirs:
                return os.path.join(root, folder_name)
    return None

folder_path = find_folder("files_in")
if not folder_path:
    raise FileNotFoundError("The folder 'files_in' was not found in the system.")

# Get the last two updated files with the specified keyword
recent_files = FileSelection.get_recent_files(folder_path, 2, keyword="UTA_HR_MAVEXPRESS_EXPORT")
print("Last two updated files:")
for file in recent_files:
    print(file)

def remove_log_file(log_file_path):
    """
    Remove the log file if it exists.
    
    Parameters:
    log_file_path (str): The path to the log file to be removed.
    """
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
    """
    Configure the logger to write logs to the specified file.
    
    Parameters:
    log_file (str): The path to the log file.
    """
    logger.add(log_file, level="INFO")

def hash_row(row):
    """
    Generate an MD5 hash for a given row.
    
    Parameters:
    row (pd.Series): A row from a DataFrame.
    
    Returns:
    str: The MD5 hash of the row.
    """
    row_string = ','.join(map(str, row))
    return hashlib.md5(row_string.encode()).hexdigest()

def load_csv(file_path):
    """
    Load a CSV file into a DataFrame.
    
    Parameters:
    file_path (str): The path to the CSV file.
    
    Returns:
    pd.DataFrame: The loaded DataFrame.
    """
    return pd.read_csv(file_path)

def add_hash_column(df):
    """
    Add a hash column to the DataFrame.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame.
    
    Returns:
    pd.DataFrame: The DataFrame with an added hash column.
    """
    df['hash'] = df.apply(hash_row, axis=1)
    return df

def get_differences(today_df, yesterday_df):
    """
    Identify new, removed, and modified entries between two DataFrames.
    
    Parameters:
    today_df (pd.DataFrame): The DataFrame for today.
    yesterday_df (pd.DataFrame): The DataFrame for yesterday.
    
    Returns:
    tuple: Lists of new entries, removed entries, and modified entries.
    """
    today_dict = dict(zip(today_df['Employee ID'], today_df['hash']))
    yesterday_dict = dict(zip(yesterday_df['Employee ID'], yesterday_df['hash']))
    
    new_entries = [emp_id for emp_id in today_dict if emp_id not in yesterday_dict]
    removed_entries = [emp_id for emp_id in yesterday_dict if emp_id not in today_dict]
    modified_entries = [emp_id for emp_id in today_dict if emp_id in yesterday_dict and today_dict[emp_id] != yesterday_dict[emp_id]]

    logger.info(f"Modified entries reports generated successfully! Count: {len(modified_entries)}")
    logger.info(f"Removed entries reports generated successfully! Count: {len(removed_entries)}")
    logger.info(f"New entries reports generated successfully! Count: {len(new_entries)}")
    
    return new_entries, removed_entries, modified_entries

def highlight_modified_fields(today_df, yesterday_df, modified_entries):
    """
    Highlight the modified fields between two DataFrames for the given entries.
    
    Parameters:
    today_df (pd.DataFrame): The DataFrame for today.
    yesterday_df (pd.DataFrame): The DataFrame for yesterday.
    modified_entries (list): List of modified entries.
    
    Returns:
    pd.DataFrame: DataFrame containing the modified details.
    """
    modified_details = []
    
    for emp_id in modified_entries:
        today_row = today_df[today_df['Employee ID'] == emp_id].iloc[0]
        yesterday_row = yesterday_df[yesterday_df['Employee ID'] == emp_id].iloc[0]
        
        modified_fields = {column: {'yesterday': yesterday_row[column], 'today': today_row[column]}
                           for column in today_row.index if today_row[column] != yesterday_row[column]}
        
        modified_details.append({
            'Employee ID': emp_id,
            'modified_fields': modified_fields
        })
    
    logger.info(f"Modified details: {modified_details}")
    return pd.DataFrame(modified_details)

def save_to_csv(df, file1_name, file2_name, output_filename=""):
    """
    Save the DataFrame to a CSV file.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to save.
    file1_name (str): The name of the first file.
    file2_name (str): The name of the second file.
    output_filename (str): The output file name.
    """
    if df.empty:
        logger.info("No modified records to save.")
        return

    all_columns = set(df.columns) - {"Employee ID"}
    fieldnames = sorted(all_columns)

    with open(output_filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        for i in range(0, len(df) - 1, 2):
            row_old = df.iloc[i]
            row_new = df.iloc[i + 1]

            writer.writerow([f"Employee ID: {row_old['Employee ID']}"])
            
            # Filter out columns with NaN values
            valid_fieldnames = [field for field in fieldnames if not pd.isna(row_old.get(field, '')) and not pd.isna(row_new.get(field, ''))]
            writer.writerow(["filename"] + valid_fieldnames)

            file1_values = [row_new.get(field, '') for field in valid_fieldnames]
            file2_values = [row_old.get(field, '') for field in valid_fieldnames]

            writer.writerow([os.path.basename(file1_name)] + file1_values)
            writer.writerow([os.path.basename(file2_name)] + file2_values)
            writer.writerow([])

    logger.info(f"Modified records saved to {output_filename}")

def save_to_json(df, file_name):
    """
    Save the DataFrame to a JSON file.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to save.
    file_name (str): The output file name.
    """
    df.to_json(file_name, orient='records', indent=4)
    logger.info(f"Data saved to {file_name}")

def main():
    """
    Main function to compare HR data between two files and log the differences.
    """
    # Get the most recent and second most recent files
    today_file = recent_files[0]
    yesterday_file = recent_files[1]
    print(today_file)
    print(yesterday_file)
    
    # Define the log file and output directory
    base_dir = Path(__file__).resolve().parent
    log_file = base_dir / "hr_details.log"
    output_dir = base_dir / "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize variables
    modified_details = []
    csv_data = []
    modified_count = 0
    
    # Remove the existing log file and configure the logger
    remove_log_file(log_file)
    configure_logger(log_file)

    logger.info(f"File 1 name: - {Path(today_file).stem}")
    logger.info(f"File 2 name: - {Path(yesterday_file).stem}")
    logger.info(f"Log file name: - {Path(log_file).stem}")
    logger.info("HR Data Comparison Started.....")

    # Load the CSV files and add hash columns
    df_today = add_hash_column(load_csv(today_file))
    df_yesterday = add_hash_column(load_csv(yesterday_file))

    # Get the differences between the two DataFrames
    new_entries, removed_entries, modified_entries = get_differences(df_today, df_yesterday)
    
    # Process the modified entries
    for record_id in modified_entries:
        # Get the rows for the current record ID from both DataFrames
        today_row = df_today[df_today['Employee ID'] == record_id].iloc[0]
        yesterday_row = df_yesterday[df_yesterday['Employee ID'] == record_id].iloc[0]

        # Identify the modified fields between the two rows
        modified_fields = {
            column: {'yesterday': yesterday_row[column], 'today': today_row[column]}
            for column in today_row.index if today_row[column] != yesterday_row[column]
        }

        if modified_fields:
            # Log the modified fields for the current record ID
            logger.info(f"Modified fields for Employee ID: {record_id}")
            headers = ["Field", "File 1", "File 2"]
            table_data = [
                [field, changes['yesterday'], changes['today']]
                for field, changes in modified_fields.items()
                if pd.notna(changes['yesterday']) and pd.notna(changes['today'])
            ]
            logger.info("\n" + tabulate.tabulate(table_data, headers=headers, tablefmt="grid"))

            # Prepare the CSV entries for the old and new values
            csv_entry_old = {"Employee ID": record_id, **{k: v['yesterday'] for k, v in modified_fields.items()}}
            csv_entry_new = {"Employee ID": record_id, **{k: v['today'] for k, v in modified_fields.items()}}
            csv_data.extend([csv_entry_old, csv_entry_new])

            # Append the modified details to the list
            modified_details.append({'Employee ID': record_id, 'modified_fields': modified_fields})
            modified_count += 1

    # Save the new, removed, and modified entries to CSV and JSON files
    logger.info(f"New Entries: {new_entries}")
    logger.info(f"Removed Entries: {removed_entries}")
    save_to_csv(df_today[df_today['Employee ID'].isin(new_entries)], today_file, yesterday_file, os.path.join(output_dir, "new_entries.csv"))
    save_to_csv(df_yesterday[df_yesterday['Employee ID'].isin(removed_entries)], today_file, yesterday_file, os.path.join(output_dir, "removed_entries.csv"))
    modified_df = pd.DataFrame(csv_data)
    save_to_csv(modified_df, today_file, yesterday_file, os.path.join(output_dir, "modified_entries.csv"))

    save_to_json(df_today[df_today['Employee ID'].isin(new_entries)], os.path.join(output_dir, "new_entries.json"))
    save_to_json(df_yesterday[df_yesterday['Employee ID'].isin(removed_entries)], os.path.join(output_dir, "removed_entries.json"))
    save_to_json(pd.DataFrame(csv_data), os.path.join(output_dir, "modified_entries.json"))

    # Log the shapes of the DataFrames for new, removed, and modified entries
    logger.info(f"Shape of new entries DataFrame: {df_today[df_today['Employee ID'].isin(new_entries)].shape}")
    logger.info(f"Shape of removed entries DataFrame: {df_yesterday[df_yesterday['Employee ID'].isin(removed_entries)].shape}")
    logger.info(f"Shape of modified entries DataFrame: {pd.DataFrame(csv_data).shape}")

if __name__ == "__main__":
    main()
