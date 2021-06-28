from builtins import int
import pandas as pd
from MailRetriever import MailRetriever
import msoffcrypto
from configparser import RawConfigParser
from FTP_Loader import FTP_Loader
import re
import psutil
import os
from pathlib import Path
import codecs
import xlrd
import zipfile
import shutil
from datetime import datetime


def process_killer():
    """Kills specific processes using their process name.
            Parameters: None
            Returns: None
        """
    logger("Killing processes.")
    PROCESSES = ['excel.exe']
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
    files = [i.path for i in os.scandir(f"{cwd}/Data/Input/") if i.is_file()]
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
        df['Data Sheet Name'] = df['Data Sheet Name'].fillna(str(0))
        df['Header Row'] = df['Header Row'].fillna(1)
        df['Data Start Row'] = df['Data Start Row'].fillna(2)
        df['MultiPartner Reporting'] = df['MultiPartner Reporting'].fillna("No")
        df['Subject_Has'] = df['Subject_Has'].fillna("")
        df['Password'] = df['Password'].fillna("")
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


def directory_validator(path):
    """Validates attachment directory and creates it in case it doesn't exists.
        Parameters:
            path (str) : Path to attachment directory.

        Returns:
            None
    """
    directory_exists = os.path.isdir(path)
    if not directory_exists:
        Path(path).mkdir(parents=True, exist_ok=True)


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
            # print(str(e))
            pass

    return df


def attachment_file_validator(file, partner_sheet_name, partner_header_idx, pos_identifier, invalid_extensions):
    """Validates saved attachment files and deletes invalid files.
    Parameters:
        file (str) : Path where attachments of a specific mail are saved.
        partner_sheet_name (str) : Name of the sheet to be read from given file.
        partner_header_idx (int) : Header row starting index.
        pos_identifier (str) : POS Column Name.
        invalid_extensions (list)

    Returns:
        None
    """

    pos_identifier = str(pos_identifier).lower().strip()
    file_ext = file.split('.')[-1].lower()
    file_df = None

    # Valid File Type
    if file_ext in invalid_extensions:
        logger(f"Error : Invalid file type '{file_ext}' found. Deleting it.")
        os.remove(file)
        return

    if "xlsx" in file_ext or "xls" in file_ext or "xlsm" in file_ext:
        if partner_sheet_name != 0:
            xls = xlrd.open_workbook(file, on_demand=True)
            if partner_sheet_name not in xls.sheet_names():
                partner_sheet_name = int(0)
        file_df = pd.read_excel(file, sheet_name=partner_sheet_name, skiprows=partner_header_idx - 1)
    else:
        file_df = csv_reader(file)

    # Delete invalid file
    if None is file_df:
        logger(f"Error : Deleting the file as its not readable.")
        os.remove(file)
    available_columns = [str(i).strip() for i in file_df.columns.str.lower()]
    if pos_identifier not in available_columns:
        logger(f"Error : Deleting the file as it not of type POS (expected POS column - '{pos_identifier}' not found within the file). Please check configuration file.")
        os.remove(file)
    else:
        logger(f"Attachment validated. Saving it in '{file}'.")


def multi_partner_resolver(matched_partners, mail_obj, subject):
    """ Analyzes and determines proper row for current mail. If still multiple rows exists then an exception is thrown.
    Parameters:
        matched_partners (dataframe) : Datatable with rows that contain partner info based on mail ids.
        mail_obj (Mail Object) : Mail object consisting of all details regarding current mail.
        subject (str) : Subject of current mail.

    Returns:
        Dataframe with only 1 valid partner.
    """
    data = list()
    output_df = None
    for row in matched_partners.iterrows():
        row = row[1]
        valid_row = False
        subject_keywords = row["Subject_Has"]
        attachment_keywords = row["Filename_Has"]
        if subject_keywords and subject_keywords in subject:
            valid_row = True
        if attachment_keywords:
            attachment_content = [i for i in mail_obj.walk() if i.get_content_maintype() != 'multipart' and
                                  i.get("Content-Disposition") is not None and
                                  i.get_content_maintype() != 'image']
            for part in attachment_content:
                attachment_name = part.get_filename()
                if attachment_keywords in attachment_name:
                    valid_row = True
                    break
        if valid_row:
            data.append(list(row))

    output_df = pd.DataFrame(data, columns=matched_partners.columns)

    # Check if we have exactly 1 row available or not
    if output_df.shape[0] > 1:
        raise Exception(f"Multiple Partners matched for the mail with subject - {subject}. Cannot resolve.")
    elif output_df.shape[0] == 0:
        raise Exception(f"No Partners the mail with subject - {subject} on basis of Subject and File Name.")

    return output_df


def partner_validator(matched_rows, subject, decoded_mail):
    """Validates the extracted partner details for current mail:
        Parameters:
            matched_rows (dataframe) : Datatable with rows that contain partner info based on mail ids.
            subject (str) : Subject of current mail.
            decoded_mail (Mail Object) : Mail object consisting of all details regarding current mail.

        Returns:
            Dataframe with only 1 valid partner.
    """
    output_dict = dict()

    # Handling no partner and multi-partners scenario.

    # Extracting File Name and removing Unicode characters (Some mail might contain this.).
    attachment_names = [i.get_filename().replace('\u200f', '') for i in decoded_mail.walk()
                        if i.get_content_maintype() != 'multipart'
                        # and i.get("Content-Disposition") is not None
                        and i.get_filename() is not None
                        and i.get_content_maintype() != 'image']

    if not attachment_names:
        logger("No attachments available in current mail.")
        return None

    if matched_rows.shape[0] == 0:
        logger("No partner found for this mail.")
        output_dict = None
    elif matched_rows.shape[0] > 1:
        subject_keywords = list(matched_rows["Subject_Has"])
        attachment_keywords = list(matched_rows["Filename_Has"])
        if not subject_keywords and not attachment_keywords:
            logger("Multiple partners identified. Please process this mail manually.")
        for attachment in attachment_names:

            for row in matched_rows.iterrows():
                row = row[1]
                valid_subject = False
                valid_attachment_name = False
                subject_keywords = row["Subject_Has"]
                attachment_keywords = row["Filename_Has"]

                # Validate Subject
                if subject_keywords:
                    if subject_keywords in subject:
                        valid_subject = True
                else:
                    valid_subject = True

                # Validate Attachment Name
                if attachment_keywords:
                    if attachment_keywords.lower() in attachment.lower():
                        valid_attachment_name = True
                else:
                    valid_attachment_name = True

                if valid_attachment_name and valid_subject:
                    partner_name = row['Submitter Name']
                    output_dict[attachment] = partner_name
    else:
        partner_name = list(matched_rows['Submitter Name'])[0]
        for attachment in attachment_names:
            logger(f"For attachment(s) '{attachment}' identified partner is {partner_name}")
            output_dict[attachment] = partner_name

    if len(output_dict.keys()) == 0 and matched_rows.shape[0] > 1:
        attachment_names_joined = ', '.join(attachment_names)
        logger(f"For attachment(s) '{attachment_names_joined}' multi partner resolution failed. Please check and update PolyAuditConfig.xlsx")
    return output_dict


def excel_decryptor(file_path, password):
    """Unlocks Excel based on password provided in Partner Config.
        Parameters:
            file_path (str): Path of locked excel file.
            password (str) : Password to unlock the excel file.

        Returns:
            None
    """
    file_name = os.path.basename(file_path)
    folder = os.path.dirname(file_path)
    final_path = os.path.join(folder, "Temp_" + file_name)

    try:

        with open(file_path, 'rb') as encrypted:
            file = msoffcrypto.OfficeFile(encrypted)
            file.load_key(password=password)
            with open(final_path, "wb") as file_obj:
                file.decrypt(file_obj)

        os.remove(file_path)
        shutil.move(final_path, file_path)

        return "File decrypted successfully."

    except Exception as e:
        if final_path and os.path.exists(final_path):
            os.remove(final_path)
        if 'The file could not be decrypted with this password' in str(e):
            return "Unable to unlock excel file. Please check password."
        else:
            return "File not locked. Please modify config file."


def zip_extractor(file, partner_file_path):
    with zipfile.ZipFile(file, 'r') as zip_obj:
        zip_files = zip_obj.namelist()
        if len(zip_files) > 1:
            logger("More than 1 file available in zip. Please process this mail manually.")
            os.remove(file)
            return None
        zip_obj.extractall(partner_file_path)
    os.remove(file)
    return os.path.join(partner_file_path, zip_files[0])


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

    # Read Config file

    config = RawConfigParser()
    config.read("config.ini")
    HOST = config.get('FTP Credentials', 'HOST')
    PORT = int(config.get('FTP Credentials', 'PORT'))
    USERNAME = config.get('FTP Credentials', 'USERNAME')
    PASSWORD = config.get('FTP Credentials', 'PASSWORD')
    imap_port = config.getint('IMAP_DETAILS', 'IMAP_PORT')
    imap_host = config.get('IMAP_DETAILS', 'IMAP_HOST')
    imap_mail_id = config.get('IMAP_DETAILS', 'IMAP_MAIL_ID')
    imap_mail_password = config.get('IMAP_DETAILS', 'IMAP_MAIL_PASSWORD')
    imap_mail_folder = config.get('IMAP_DETAILS', 'IMAP_FOLDER')
    TOTAL_KEYWORDS = config.get('Total Keywords', 'KEYWORDS').split(',')
    INVALID_FILE_EXTENSIONS = config.get('Invalid File Extension', 'KEYWORDS').split(',')

    # Initialize FTP Module
    ftp_obj = FTP_Loader(host=HOST, user_name=USERNAME, password=PASSWORD, port=PORT)

    # Downloading the files for processing.
    logger("Downloading Config, OBIIE, Model N files.")
    available_files = [file for file in ftp_obj.ftp_getfiles() if file.endswith(".xlsx") or file.endswith(".csv")]
    available_files = [i for i in available_files if 'Config' in i or '.csv' in i]

    for file in available_files:
        file = re.findall(r"(?<=:\d{2}\s).+", file)[0]
        cwd = os.getcwd()
        destination = f"{cwd}/Data/Input/{file}"
        try:
            ftp_obj.ftp_downloadfile(file, destination)
        except:
            ftp_obj = FTP_Loader(host=HOST, user_name=USERNAME, password=PASSWORD, port=PORT)
            ftp_obj.ftp_downloadfile(file, destination)

    # Reading Poly Audit Config
    cwd = os.getcwd()
    files = [i.path for i in os.scandir(f"{cwd}/Data/Input/") if i.is_file()]
    CONFIG_FILE = [i for i in files if 'Config' in i][0]
    df_partnerMaster = read_audit_configuration(CONFIG_FILE)


    # Getting New Mails
    mail_obj = MailRetriever(imap_port, imap_host, imap_mail_id, imap_mail_password, imap_mail_folder)
    mail_obj.mail_connection_setter()
    mails = mail_obj.mail_fetcher()

    # Processing New Mails
    for num in mails[0].split():
        try:
            logger("\n")
            logger("-"*50)

            decoded_mail = mail_obj.mail_decoder(num)
            subject = [i[1] for i in decoded_mail.items() if 'subject' in i[0].lower()][0]
            sender_mail_id = mail_obj.id_retriever(decoded_mail).lower()

            logger(f"Processing Mail with subject - {subject}")
            # Extract Partner rows based on Sender's Mail ID
            rows_filter = df_partnerMaster['Partner Mail-Id'].str.lower().str.contains(sender_mail_id, na=False)
            matched_rows = df_partnerMaster[rows_filter]
            if matched_rows.shape[0] == 0:
                logger(f"Email - {sender_mail_id} cannot be mapped to any partner. Please update PartnerMasterConfig.xlsx.")
                mail_obj.mark_as_read(num)
                continue

            # Generate Attachment and Partner Mapping. Determining which attachment belongs to which folder.
            attachment_data = partner_validator(matched_rows, subject, decoded_mail)
            if None is attachment_data:
                mail_obj.mark_as_read(num)
                continue

            for attachment_name in attachment_data:
                partner_name = attachment_data[attachment_name]
                partner_row_filter = matched_rows['Submitter Name'] == partner_name
                partner_details = matched_rows[partner_row_filter]
                pos_identifier = list(partner_details['POS Identifier'])[0]
                partner_file_path = list(partner_details['Partner File Path'])[0]
                partner_sheet_name = list(partner_details['Data Sheet Name'])[0]
                partner_header_idx = list(partner_details['Header Row'])[0]
                attachment_password = list(partner_details['Password'])[0]
                if type(attachment_password) == float or type(attachment_password) == int:
                    attachment_password = str(int(attachment_password))

                # Validate Directory and create if it does not exists.
                directory_validator(partner_file_path)

                # Save Save attachments in extracted directory.
                mail_obj.PARENT_DIRECTORY = partner_file_path
                saved_attachments = mail_obj.attachment_saver(decoded_mail, attachment_name)

                for file_idx, file in enumerate(saved_attachments):
                    logger(f"Processing attachment - ({os.path.basename(file)})")

                    # Extract Zip File
                    file_extension = os.path.splitext(file)[1]
                    if 'zip' in file_extension.lower():
                        file = zip_extractor(file, partner_file_path)
                        # Multiple Files available in attachment
                        if None is file:
                            continue

                    if attachment_password:
                        decrypted_message = excel_decryptor(file, attachment_password)
                        logger(decrypted_message)

                        if 'File not locked.' in decrypted_message or \
                                'successfully' in decrypted_message:
                            pass
                        else:
                            continue

                    # Validate Attachments
                    attachment_file_validator(file, partner_sheet_name, partner_header_idx, pos_identifier, INVALID_FILE_EXTENSIONS)

        except Exception as e:
            logger(str(e))
        finally:
            mail_obj.mark_as_read(num)


    if not available_files:
        logger("No emails available to be procesed in ")

    # Remove Input Files
    remove_input_files()


if __name__ == "__main__":
    logger("Starting Mail Processor at - " + str(datetime.now()))
    processor()
