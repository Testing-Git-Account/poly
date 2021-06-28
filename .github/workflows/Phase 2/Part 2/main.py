import pandas as pd
from FTP_Loader import FTP_Loader
from configparser import RawConfigParser
import psutil
import os
import re
from OBIEEProcessor import OBIEEProcessor
from pathlib import Path
import codecs
import xlrd


def process_killer():
    """Kills specific processes using their process name.
            Parameters: None
            Returns: None
        """
    logger("Killing processes.")
    PROCESSES = ['excel']
    for proc in psutil.process_iter():
        if proc.name().lower() in PROCESSES:
            proc.kill()


def logger(txt):
    """Prints and logs message(s) on console and in a log file respectively.
        Parameters:
            txt: Logging Message.
        Returns:
            None
    """
    print(txt)
    if os.path.isfile("Data/Output Files/Process_Log.txt"):
        write_mode = 'a'
    else:
        write_mode = 'w'

    with open("Data/Output Files/Process_Log.txt", write_mode) as file_obj:
        file_obj.write("\n"+str(txt))


def remove_input_files():
    """Clears out Input Folder
                Parameters: None
                Returns: None
    """
    cwd = os.getcwd()
    files = [i.path for i in os.scandir(f"{cwd}/Data/Input Files/") if i.is_file()]
    for file in files:
        os.remove(file)


def remove_output_files():
    """Clears out Output Folder
            Parameters: None
            Returns: None
    """
    cwd = os.getcwd()
    files = [i.path for i in os.scandir(f"{cwd}/Data/Output Files/") if i.is_file()]
    for file in files:
        os.remove(file)


def read_audit_configuration(config_file_path):
    """Reads Poly Audit Configuration file.
        Parameters:
            config_file_path (str) : Path to Poly Audit config file.

        Returns:
            Dataframe consisting poly audit config data.
    """
    try:
        list_data_header_column = ['Header Row', 'Data Start Row']
        df = pd.read_excel(config_file_path)
        df['Data Sheet Name'] = df['Data Sheet Name'].fillna(0)
        df['Header Row'] = df['Header Row'].fillna(1)
        df['Data Start Row'] = df['Data Start Row'].fillna(2)
        df['MultiPartner Reporting'] = df['MultiPartner Reporting'].fillna("No")
        df['Subject_Has'] = df['Subject_Has'].fillna("")
        df['Filename_Has'] = df['Filename_Has'].fillna("")
        df['Colm-Header'] = df['Colm-Header'].fillna("")
        for col in list_data_header_column:
            try:
                df[col] = pd.to_numeric(df[col],downcast='integer')
            except Exception as col_exception:
                logger("Non Integer value found in column :" + col)
                raise
    except Exception as config_exception:
        logger("Some exception occurred in Poly Audit config file :" + str(config_exception))
        raise
    return df


def csv_reader(file):
    """Reads CSV file.
        Parameters:
            file (str) : Path of CSV file to be read.
        Returns:

            Dataframe of read file.
    """
    list_deli = [',', '\t', '|', ':', ';']
    df = None
    for deli in list_deli:
        try:
            if deli != '\t' and not file.lower().endswith('txt'):
                try:
                    df = pd.read_csv(file, sep=deli, engine='python')
                except Exception as e:
                    df = pd.read_csv(file, sep=deli, encoding='ISO-8859-1')
            else:
                if file.lower().endswith('txt'):
                    df = pd.read_csv(file, sep=deli, encoding='ISO-8859-1')
                else:
                    doc = codecs.open(file, 'rU', 'UTF-16')
                    df = pd.read_csv(doc, sep=deli)
                    doc.close()

            if df.shape[1] > 1:
                break
        except Exception as e:
            print(str(e))

    return df


def processor():
    """Validates the extracted partner details for current mail:
        Parameters: None

        Returns: None
    """

    # Remove Input Files
    remove_input_files()

    # Killing processes
    process_killer()

    # Remove Output Files
    remove_output_files()


    # Read data from config.ini
    config = RawConfigParser()
    config.read("config.ini")

    HOST = config.get('FTP Credentials', 'HOST')
    PORT = int(config.get('FTP Credentials', 'PORT'))
    USERNAME = config.get('FTP Credentials', 'USERNAME')
    PASSWORD = config.get('FTP Credentials', 'PASSWORD')
    TOTAL_KEYWORDS = config.get('Total Keywords', 'KEYWORDS').split(',')

    ftp_obj = FTP_Loader(host=HOST, user_name=USERNAME, password=PASSWORD, port=PORT)

    # Downloading the files for processing.
    logger("Downloading Config, OBIIE, Model N files.")
    available_files = [file for file in ftp_obj.ftp_getfiles() if file.endswith(".xlsx") or file.endswith(".csv")]
    available_files = [i for i in available_files if 'Config' in i or '.csv' in i]

    for file in available_files:
        file = re.findall(r"(?<=:\d{2}\s).+", file)[0]
        cwd = os.getcwd()
        destination = f"{cwd}/Data/Input Files/{file}"
        try:
            ftp_obj.ftp_downloadfile(file, destination)
        except:
            ftp_obj = FTP_Loader(host=HOST, user_name=USERNAME, password=PASSWORD, port=PORT)
            ftp_obj.ftp_downloadfile(file, destination)

    # Reading Poly Audit Config
    cwd = os.getcwd()
    files = [i.path for i in os.scandir(f"{cwd}/Data/Input Files/") if i.is_file()]
    CONFIG_FILE = [i for i in files if 'Config' in i][0]
    df_partnerMaster = read_audit_configuration(CONFIG_FILE)
    OBIIE_FILE = [i for i in files if ".csv" in i][0]

    # Get Partner Details
    # df_PartnerMaster = read_audit_configuration(CONFIG_FILE)

    # Initialize Objects
    OBIEEProcessor_obj = OBIEEProcessor(OBIIE_FILE)
    OBIEE_df = OBIEEProcessor_obj.df_OBIEE_Data
    
    # Get list of folders
    raw_attachments_directory = os.path.dirname(df_partnerMaster['Partner File Path'][0])
    partner_folders = os.scandir(raw_attachments_directory)

    for folder in partner_folders:
        logger("\n")
        logger("-" * 50)

        folder_path = folder.path
        partner_files = [i.path for i in os.scandir(folder)]
        if len(partner_files) == 0:
            logger(f"No files available in folder - {folder_path}")
            os.rmdir(folder_path)
            continue

        folder = os.path.basename(folder)
        logger(f"Processing folder - {folder}")

        # Extract Partner Details
        partner_filter = df_partnerMaster['Partner File Path'].str.contains(folder)
        partner_config_data = df_partnerMaster[partner_filter]
        if partner_config_data.shape[0] == 0:
            logger(f"Cannot find folder ({folder}) info in Partner Config. Please process the files of this folder manually.")
            continue
        partner_name = list(partner_config_data['Submitter Name'])[0]
        partner_quarter = list(partner_config_data["Current Fiscal Quarter"])[0]
        logger(f"Partner = {partner_name}")

        # Extract OBIEE Data
        formatted_partner_name = partner_name.lower().replace('(', r'\(').replace(')', r'\)')
        OBIEE_filter = OBIEE_df['Submitter Name'].str.lower().str.contains(formatted_partner_name)
        partner_obiee_data = OBIEE_df[OBIEE_filter]
        if partner_obiee_data.shape[0] == 0:
            logger(f"No OBIEE Data available for partner - '{partner_name}'")
            continue
        partner_oracle_id = list(partner_obiee_data['Submitter Oracle ID'])[0]

        # Process all files for current partner. Create {File-ID:File-Name} dictionary for partner.
        partner_file_names = list(set(partner_obiee_data['Partner Reported File Name']))
        file_id_name_mapping = {i[0:8]: i[9:] for i in partner_file_names}

        # Process files not available in OBIEE
        unavailable_files = [os.path.basename(i) for i in partner_files if os.path.basename(i) not in file_id_name_mapping.values()]
        if len(unavailable_files) > 0:
            logger(f"Could not move {', '.join(unavailable_files)} as there is no record available in OBIEE")

        # Process available files.
        for file_id in file_id_name_mapping:
            file_name = file_id_name_mapping[file_id]
            matching_file = [i for i in partner_files if os.path.basename(i) in file_name]
            
            # There should be only 1 record for a file in OBIEE file.
            if len(matching_file) != 1:
                continue

            file_name = matching_file[0]

            # Create FTP Folder Path
            ftp_path = os.path.join('Poly Audit Phase 2 Test (Do not use)', partner_quarter, partner_oracle_id, file_id)

            # Validate directory and upload file.
            ftp_obj = FTP_Loader(host=HOST, user_name=USERNAME, password=PASSWORD, port=PORT)
            folder_available = ftp_obj.directory_validator(ftp_path)

            if not folder_available:
                ftp_obj.directory_creator(ftp_path)
            else:
                available_files = ftp_obj.files_extractor()
                if len(available_files) > 0:
                    files = ', '.join(available_files)
                    logger(f'File(s) - {files} already available in FTP path - {ftp_path}')
                    os.remove(os.path.join(folder_path, file_name))
                    continue

            file_path = os.path.join(folder_path, file_name)
            logger(f"File '{os.path.basename(file_name)}' moved to - '{ftp_path}'")
            ftp_obj.file_uploader(file_path)
            ftp_obj.ftp_close()
            os.remove(file_path)

        # Files remaining in partner's folder?
        remaining_files = [i.path for i in os.scandir(folder_path)]
        if len(remaining_files) == 0:
            os.rmdir(folder_path)


if __name__ == "__main__":
    processor()
