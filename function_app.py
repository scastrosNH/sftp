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


# read flexible Csv3 only when origin file is Transformador (as it has a different structure)
def read_flexible_csv3(file_handle):
    # Read from file handle and decode as necessary
    content = file_handle.read().decode('utf-8')
    columns = [
        "GatewaySN", "DeviceName", "DeviceLocalID", "DeviceTypeID","LocalTimeStamp","TotalDemandMaxReactivePower",
        "TotalDemandMaxActivePower","RmsFrequency","RmsCurrentPhsA","AverageRmsCurrent","RmsCurrentPhsB","RmsCurrentPhsC",
        "RmsCurrentGround","RmsCurrentNeut","TotalRmsPowerFactor","FundThdCurrentPhsA","FundThdCurrentPhsB","FundThdCurrentPhsC",
        "FundThdVoltagePhsAB","FundThdVoltagePhsAN","FundThdVoltagePhsBC","FundThdVoltagePhsBN","FundThdVoltagePhsCA","FundThdVoltagePhsCN",
        "RmsVoltagePhsAB","TotalDeliveredApparentEnergy","RmsVoltagePhsAN","RmsReactivePowerPhsA","RmsReactivePowerPhsB","RmsReactivePowerPhsC", 
        "TotalDeliveredReactiveEnergy","TotalReceivedReactiveEnergy","TotalRmsReactivePower","RmsApparentPowerPhsA","RmsApparentPowerPhsB","RmsApparentPowerPhsC",
        "TotalRmsApparentPower","RmsVoltagePhsBC","RmsVoltagePhsBN","RmsVoltagePhsCA","RmsVoltagePhsCN","AverageRmsVoltagePhsToPhs",
        "RmsActivePowerPhsA","RmsActivePowerPhsB","RmsActivePowerPhsC","TotalDemandActivePower","TotalDeliveredActiveEnergy","TotalReceivedActiveEnergy",
        "TotalRmsActivePower"       
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
            new_row = [
                metadata.get("Gateway SN", None),
                metadata.get("Device Name", None),
                metadata.get("Device Local ID", None),
                metadata.get("Device Type ID", None)
            ] + row[2:len(columns)]
            
            data.append(new_row)
            # Debugging: Print the data appended to ensure it's correct
            #print("Appended row:", new_row)
        
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    print(df)

    # Replace NaN with 0 for all numeric columns
    numeric_columns = [
        "TotalDemandMaxReactivePower","TotalDemandMaxActivePower","RmsFrequency","RmsCurrentPhsA","AverageRmsCurrent","RmsCurrentPhsB",
        "RmsCurrentPhsC","RmsCurrentGround","RmsCurrentNeut","TotalRmsPowerFactor","FundThdCurrentPhsA","FundThdCurrentPhsB",
        "FundThdCurrentPhsC","FundThdVoltagePhsAB","FundThdVoltagePhsAN","FundThdVoltagePhsBC","FundThdVoltagePhsBN","FundThdVoltagePhsCA",
        "FundThdVoltagePhsCN","RmsVoltagePhsAB","TotalDeliveredApparentEnergy","RmsVoltagePhsAN","RmsReactivePowerPhsA","RmsReactivePowerPhsB",
        "RmsReactivePowerPhsC", "TotalDeliveredReactiveEnergy","TotalReceivedReactiveEnergy","TotalRmsReactivePower","RmsApparentPowerPhsA","RmsApparentPowerPhsB",
        "RmsApparentPowerPhsC","TotalRmsApparentPower","RmsVoltagePhsBC","RmsVoltagePhsBN","RmsVoltagePhsCA","RmsVoltagePhsCN",
        "AverageRmsVoltagePhsToPhs","RmsActivePowerPhsA","RmsActivePowerPhsB","RmsActivePowerPhsC","TotalDemandActivePower","TotalDeliveredActiveEnergy",
        "TotalReceivedActiveEnergy","TotalRmsActivePower"
    ]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)
    return df

def is_transformador_file(filename):
    # Use a regular expression to check if 'Transformador' is exactly part of the filename pattern you expect
    pattern = r'Transformador'  # \b represents a word boundary in regex
    return re.search(pattern, filename, re.IGNORECASE) is not None

def should_exclude_file(filename):
    # Patterns for 'transformador' or 'montacargas'
    exclude_patterns = [r'_DOSIFICADO_', r'_VACEO_',r'_MOLINO 2_',r'_MOLINO QUEBRANTADOR_',r'_EMPAQUE_',r'_MOLINO EXTRUDER_',r'_EXTRUDER_',r'_PELETIZADORA 1_',r'_PELETIZADORA 2_', r'_BATERIA MONTACARGAS_']
    return any(re.search(pattern, filename, re.IGNORECASE) for pattern in exclude_patterns)
    
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
            if should_exclude_file(file):
                logging.info(f"Skipping file due to filter: {file}")
                continue
            else:
                logging.info(f"Processing standard file: {file}")
                df = read_flexible_csv3(file_handle)
            
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
            final_df.to_sql('EnergyTestTransformador', con=engine, if_exists='append', index=False)
            logging.info('Data pushed to Azure SQL Database successfully.')
        else:
            logging.error("No valid dataframes available to concatenate; possibly all DataFrames were empty or all-NA.")
            raise ValueError("No objects to concatenate")
    else:
        logging.info("No new files, nothing new uploaded.")
