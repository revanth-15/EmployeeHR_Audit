import os
from datetime import datetime

def get_recent_files(folder_path, n, keyword):
    """
    Get a list of the most recently updated files in the specified folder.

    Parameters:
    folder_path (str): The path to the folder containing the files.
    n (int): The number of recent files to return.
    keyword (str): A keyword to filter the files by name.

    Returns:
    list: A list of the most recently updated files.
    """
    # Get a list of files in the specified folder
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    
    # Filter files by keyword if provided
    if keyword:
        files = [f for f in files if keyword in os.path.basename(f)]

    # Sort the files by modification time
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    
    # Return the last n updated files
    return files[:n]

if __name__ == "__main__":
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
    recent_files = get_recent_files(folder_path, 2, keyword='')

    print("Last two updated files:")
    for i, file in enumerate(recent_files):
        # Get the file name without extension and the modification date
        file_date = os.path.basename(file).split('_')[-1].split('.')[0]
        name_without_extension = os.path.splitext(file_date)[0]
        print(f"Date: {name_without_extension}")

        # Get the modification time of the file
        mod_time = os.path.getmtime(file)
        mod_time_str = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d')
        print(f"Last updated file: {file}, Modification date and time: {mod_time_str}")

        try:
            time_format = bool(datetime.strptime(name_without_extension, mod_time_str))
        except ValueError:
            print(f"File name {file} does not match the expected date format.")
            continue
        # Check if the file name matches the modification date
        if time_format == True:
            # Check if the file name matches the modification date
            if mod_time_str == name_without_extension:
                print("File name matches the modification date.")
            else:
                print("File name does not match the modification date.")
        else:
            print("File name does not have the correct format.")
else:
    print("No files found in the specified folder.")
