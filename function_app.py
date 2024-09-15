import logging
import azure.functions as func
import pandas as pd
import paramiko
from sqlalchemy import create_engine
import os
import datetime
import pytz
import csv
import pyodbc
import re

app = func.FunctionApp()

# Define SQL connection string securely
sql_connection_string = os.getenv('SQL_CONNECTION_STRING')

##"0 */10 * * * *"
@app.schedule(schedule="0 */10 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
def cron_PAS800(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        # Set up logging
    
    # Set up logging
        logging.basicConfig(level=logging.INFO)
    
    # Establish SFTP connection
    sftp_host = os.getenv("SFTP_HOST")
    sftp_username = os.getenv("SFTP_USERNAME")
    sftp_password = os.getenv("SFTP_PASSWORD")
    sftp_port = 22 
    
    transport = paramiko.Transport((sftp_host, sftp_port))
    try:
        transport.connect(username=sftp_username, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info("SFTP successful connection")
        batch_process_files(sftp)
    except paramiko.SSHException as e:
        logging.error(f"SSH/SFTP operation failed: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        sftp.close()
        transport.close()

def read_flexible_csv(file_handle,filename):
    # Read from file handle and decode as necessary
    content = file_handle.read().decode('utf-8')
    columns = [
        "GatewaySN", "DeviceName", "DeviceLocalID", "DeviceTypeID", "LocalTimeStamp",
        "RmsFrequency", "RmsCurrentPhsA", "RmsCurrentPhsB", "RmsCurrentPhsC",
        "TotalRmsPowerFactor", "RmsVoltagePhsAB", "RmsVoltagePhsBC", "RmsVoltagePhsCA",
        "RmsActivePowerPhsA", "RmsActivePowerPhsB", "RmsActivePowerPhsC",
        "TotalDeliveredActiveEnergy", "TotalRmsActivePower", 
    ]
    data = []
    metadata = {}
    reader = csv.reader(content.splitlines())
    print(reader)
    for i, row in enumerate(reader):
        if i == 1:  # First line contains some metadata
            metadata = {
                "Gateway SN": row[1],
                "Device Name": row[4],
                "Device Local ID": row[5],
                "Device Type ID": row[6]
            }
        elif i == 6:  # The header line we need is the 7th line (index 6)
            continue
        elif i > 6:  # Process rows after the header
            if "MONTACARGAS" in filename:
                new_row = [
                    metadata.get("Gateway SN", None),
                    metadata.get("Device Name", None),
                    metadata.get("Device Local ID", None),
                    metadata.get("Device Type ID", None),
                    row[2],
                    " "
                ] + row[3:len(columns) - 2]
            else:
                new_row = [
                    metadata.get("Gateway SN", None),
                    metadata.get("Device Name", None),
                    metadata.get("Device Local ID", None),
                    metadata.get("Device Type ID", None)
                ] + row[2:len(columns) - 2]
            
            data.append(new_row)
            # Debugging: Print the data appended to ensure it's correct
            #print("Appended row:", new_row)
        
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    print(df)

# read flexible Csv2 only when origin file is DOSIFICADO (as it has a different structure)
def read_flexible_csv2(file_handle):
    # Read from file handle and decode as necessary
    content = file_handle.read().decode('utf-8')
    columns = [
        "GatewaySN", "DeviceName", "DeviceLocalID", "DeviceTypeID", "LocalTimeStamp",
        "RmsFrequency", "RmsCurrentPhsA", "RmsCurrentPhsB", "RmsCurrentPhsC",
        "TotalRmsPowerFactor", "RmsVoltagePhsAB", "RmsVoltagePhsBC", "RmsVoltagePhsCA",
        "RmsActivePowerPhsA", "RmsActivePowerPhsB", "RmsActivePowerPhsC",
        "TotalDeliveredActiveEnergy", "TotalRmsActivePower", "RmsMaxCurrentPhsC","RmsCurrentNeut" 
    ]
    data = []
    metadata = {}
    reader = csv.reader(content.splitlines())
    column_order = [4, 18, 5, 6, 7, 19, 16, 17]  # new column order for DOSIFICADO

    for i, row in enumerate(reader):
        if i == 1:  # First line contains some metadata
            metadata = {
                "Gateway SN": row[1],
                "Device Name": row[4],
                "Device Local ID": row[5],
                "Device Type ID": row[6]
            }
        elif i == 6:  # The header line we need is the 7th line (index 6)
            continue
        elif i > 6:  # Process rows after the header

            # Initialize a row with metadata and zeros for all data columns
            new_row = [
                metadata.get("Gateway SN", None),
                metadata.get("Device Name", None),
                metadata.get("Device Local ID", None),
                metadata.get("Device Type ID", None)
            ] + [0] * (len(columns) - 4)

            # Map the values starting from the third item to specified columns
            for idx, col_index in enumerate(column_order):
                if idx + 2 < len(row):  # Ensure there's enough data in the row
                    new_row[col_index] = row[idx + 2]

            data.append(new_row)
        
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)

    # Replace NaN with 0 for all numeric columns
    numeric_columns = [
        "RmsFrequency", "RmsCurrentPhsA", "RmsCurrentPhsB", "RmsCurrentPhsC",
        "TotalRmsPowerFactor", "RmsVoltagePhsAB", "RmsVoltagePhsBC", "RmsVoltagePhsCA",
        "RmsActivePowerPhsA", "RmsActivePowerPhsB", "RmsActivePowerPhsC",
        "TotalDeliveredActiveEnergy", "TotalRmsActivePower","RmsMaxCurrentPhsC","RmsCurrentNeut" 
    ]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)
    
    #division of Energy and power to K
    df["TotalDeliveredActiveEnergy"] = df["TotalDeliveredActiveEnergy"] / 1000
    df["TotalRmsActivePower"] = df["TotalRmsActivePower"] / 1000
    return df

# read flexible Csv3 only when origin file is Transformador (as it has a different structure)
def read_flexible_csv3(file_handle):
    # Read from file handle and decode as necessary
    content = file_handle.read().decode('utf-8')
    columns = [
        "GatewaySN", "DeviceName", "DeviceLocalID", "DeviceTypeID", "LocalTimeStamp",
        "RmsFrequency", "RmsCurrentPhsA", "RmsCurrentPhsB", "RmsCurrentPhsC",
        "TotalRmsPowerFactor", "RmsVoltagePhsAB", "RmsVoltagePhsBC", "RmsVoltagePhsCA",
        "RmsActivePowerPhsA", "RmsActivePowerPhsB", "RmsActivePowerPhsC",
        "TotalDeliveredActiveEnergy", "TotalRmsActivePower", "RmsMaxCurrentPhsC","RmsCurrentNeut" 
    ]
    data = []
    metadata = {}
    reader = csv.reader(content.splitlines())
    # mapping Transformador
    input_to_output_map = {
        0: 4,   # Local Time Stamp
        3: 5,   # RmsFrequency (Hz)
        4: 6,   # RmsCurrentPhsA (A)
        6: 7,   # RmsCurrentPhsB (A)
        7: 8,   # RmsCurrentPhsC (A)
        10: 9,  # TotalRmsPowerFactor
        20: 10, # RmsVoltagePhsAB (V)
        33: 11, # RmsVoltagePhsBC (V)
        35: 12, # RmsVoltagePhsCA (V)
        38: 13, # RmsActivePowerPhsA (W)
        39: 14, # RmsActivePowerPhsB (W)
        40: 15, # RmsActivePowerPhsC (W)
        42: 16, # TotalDeliveredActiveEnergy (Wh)
        44: 17, # TotalRmsActivePower (W)
        9: 19   # RmsCurrentNeut (A)
    }

    for i, row in enumerate(reader):
        if i == 1:  # First line contains some metadata
            metadata = {
                "Gateway SN": row[1],
                "Device Name": row[4],
                "Device Local ID": row[5],
                "Device Type ID": row[6]
            }
        elif i == 6:  # The header line we need is the 7th line (index 6)
            continue
        elif i > 6:  # Process rows after the header
            # Initialize a row with metadata and zeros for all data columns
            new_row = [
                metadata.get("Gateway SN", None),
                metadata.get("Device Name", None),
                metadata.get("Device Local ID", None),
                metadata.get("Device Type ID", None)
            ] + [0] * (len(columns) - 4)

             # Map the input row values to the new row based on the mapping
            for input_idx, output_idx in input_to_output_map.items():
                # Adjust index because input starts from column 2 (indexing from 0)
                if input_idx + 2 < len(row):  # Ensure the index is within the row length
                    new_row[output_idx] = row[input_idx + 2]  # Map the value to the correct position

            # Append the newly structured row to the data list
            data.append(new_row)
        
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)

    # Replace NaN with 0 for all numeric columns
    numeric_columns = [
        "RmsFrequency", "RmsCurrentPhsA", "RmsCurrentPhsB", "RmsCurrentPhsC",
        "TotalRmsPowerFactor", "RmsVoltagePhsAB", "RmsVoltagePhsBC", "RmsVoltagePhsCA",
        "RmsActivePowerPhsA", "RmsActivePowerPhsB", "RmsActivePowerPhsC",
        "TotalDeliveredActiveEnergy", "TotalRmsActivePower","RmsMaxCurrentPhsC","RmsCurrentNeut" 
    ]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)
    return df

def is_dosificado_file(filename):
    # Use a regular expression to check if 'DOSIFICADO' is exactly part of the filename pattern you expect
    pattern = r'_DOSIFICADO_'  # \b represents a word boundary in regex
    return re.search(pattern, filename, re.IGNORECASE) is not None

def is_transformador_file(filename):
    # Use a regular expression to check if 'Transformador' is exactly part of the filename pattern you expect
    pattern = r'Transformador'  # \b represents a word boundary in regex
    return re.search(pattern, filename, re.IGNORECASE) is not None
    
def fetch_recent_files(sftp, minutes):
    # Get the current time
    current_time = datetime.datetime.now()
    print("Current time:", current_time)  # Debug: Show current time
    
    recent_files = []

    # Fetching file attributes
    for file_attr in sftp.listdir_attr():
        file_mtime = datetime.datetime.fromtimestamp(file_attr.st_mtime)

        if (current_time - file_mtime) <= datetime.timedelta(minutes=minutes):
            recent_files.append(file_attr.filename)
            #print(f"Added {file_attr.filename} to recent files.")  # Debug: Show files added to the list
        else:
            #print(f"Skipped {file_attr.filename} as it is not modified in the last {minutes} minutes.")  # Debug: Show skipped files
            continue
    return recent_files

def batch_process_files(sftp):
    files = fetch_recent_files(sftp,10)
    new_files_found = True #False
    dataframes = []  # List to store DataFrames

    if len(files) > 0:
        new_files_found = True
    for file in files:
        #logging.info(f"Checking file: {file}")  # Log the file being checked  
        with sftp.open(file, mode='r') as file_handle:
            # Determine which function to use based on file name
            if is_transformador_file(file):
                logging.info(f"Processing Transformador file: {file}")
                df = read_flexible_csv3(file_handle)
            elif is_dosificado_file(file):
                logging.info(f"Processing DOSIFICADO file: {file}")
                df = read_flexible_csv2(file_handle)
            else:
                logging.info(f"Processing standard file: {file}")
                df = read_flexible_csv(file_handle, file)
            
            # Log the DataFrame shape to check if it's empty
            logging.info(f"DataFrame shape: {df.shape}")
            logging.debug(f"DataFrame columns: {df.columns.tolist()}")
            logging.debug(f"DataFrame first few rows: {df.head()}")
            
            # Append the dataframe to the list if it is not empty and not all NA
            if not df.empty and not df.isna().all().all():
                dataframes.append(df)
            else:
                logging.warning(f"Empty or all-NA DataFrame encountered in file: {file}")
    
    if new_files_found:
        if dataframes:
            final_df = pd.concat(dataframes, ignore_index=True)
            connection_string = sql_connection_string  # Make sure this is correctly defined
            engine = create_engine(connection_string)
            final_df.to_sql('EnergyTest', con=engine, if_exists='append', index=False)
            logging.info('Data pushed to Azure SQL Database successfully.')
        else:
            logging.error("No valid dataframes available to concatenate; possibly all DataFrames were empty or all-NA.")
            raise ValueError("No objects to concatenate")
    else:
        logging.info("No new files, nothing new uploaded.")
