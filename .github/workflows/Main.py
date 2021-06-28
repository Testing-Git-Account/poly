import pandas as pd
import xlrd
from OBIEEProcessor import OBIEEProcessor
from ModelNProcessor import ModelNProcessor
from OutputFormatter import OutputFormatter
from FTP_Loader import FTP_Loader
import re
from configparser import RawConfigParser
import os
import shutil
from openpyxl import load_workbook
import codecs
import datetime
import math


def read_audit_configuration(config_file_path):
    try:
        list_data_header_column = ['Header Row', 'Data Start Row']
        df = pd.read_excel(config_file_path)
        df['Data Sheet Name'] = df['Data Sheet Name'].fillna(0)
        df['Header Row'] = df['Header Row'].fillna(1)
        df['Data Start Row'] = df['Data Start Row'].fillna(2)
        df['MultiPartner Reporting'] = df['MultiPartner Reporting'].fillna("No")
        df['Colm-Header'] = df['Colm-Header'].fillna("")
        for col in list_data_header_column:
            try:
                df[col] = pd.to_numeric(df[col],downcast='integer')
            except Exception as col_exception:
                logger("Non Integer value found in column :" + col)
                raise
    except Exception as config_exception:
        logger("Some exception ocured in Poly Audit config file :" + str(config_exception))
        raise
    return df


def model_n_processor(df, ModelNProcessor_obj):
    # Adding Model-N columns

    df['TOTAL_QUANTITY'] = ''
    df['OBIEE_QUANTITY'] = ''
    df['RESUBMIT_QUANTITY'] = ''
    df['REJECT_QUANTITY'] = ''
    df['UNACCOUNTED_QUANTITY'] = ''

    for row_index, row_value in enumerate(df.iterrows()):
        partner_file_id = row_value[1]["Partner Reported File Name"].split('-')[0]
        submitter_name = row_value[1]['Submitter Name']
        if partner_file_id == '':
            continue

        model_df = ModelNProcessor_obj.processor(partner_file_id, submitter_name)
        if model_df.shape[0] > 0:
            df.at[row_index, 'TOTAL_QUANTITY'] = model_df.iloc[0]["TOTAL_QUANTITY"]
            df.at[row_index, 'OBIEE_QUANTITY'] = model_df.iloc[0]["OBIEE_QUANTITY"]
            df.at[row_index, 'RESUBMIT_QUANTITY'] = model_df.iloc[0]["RESUBMIT_QUANTITY"] if int(
                model_df.iloc[0]["RESUBMIT_QUANTITY"]) != 0 else '0'
            df.at[row_index, 'REJECT_QUANTITY'] = model_df.iloc[0]["REJECT_QUANTITY"] if int(
                model_df.iloc[0]["REJECT_QUANTITY"]) != 0 else '0'
            df.at[row_index, 'UNACCOUNTED_QUANTITY'] = model_df.iloc[0]["UNACCOUNTED_QUANTITY"] if int(
                model_df.iloc[0]["UNACCOUNTED_QUANTITY"]) != 0 else '0'
        else:
            logger('Record not found in  Model-N file for Partner File ID - ' + partner_file_id)

    df = df.reset_index()
    df.drop(['index'], axis=1, inplace=True)

    last_row_idx = df.shape[0] - 1
    df.at[last_row_idx, 'TOTAL_QUANTITY'] = df['TOTAL_QUANTITY'].apply(lambda x: 0.0 if x=='' else x)[0:df.shape[0] - 1].sum()
    df.at[last_row_idx, 'OBIEE_QUANTITY'] = df['OBIEE_QUANTITY'].apply(lambda x: 0.0 if x=='' else x)[0:df.shape[0] - 1].sum()
    df.at[last_row_idx, 'RESUBMIT_QUANTITY'] = sum([int(i) for i in df['RESUBMIT_QUANTITY'][0:last_row_idx] if i != '-' and i != ''])
    df.at[last_row_idx, 'REJECT_QUANTITY'] = sum([int(i) for i in df['REJECT_QUANTITY'][0:last_row_idx] if i != '-' and i != ''])
    df.at[last_row_idx, 'UNACCOUNTED_QUANTITY'] = sum([int(i) for i in df['UNACCOUNTED_QUANTITY'][0:last_row_idx] if i != '-' and i != ''])

    return df


def output_writer(df):
    OUTPUT_FILE = "Output Files/Audit Process Report.xlsx"
    TEMPLATE_PATH = "Template/Output Template.xlsx"
    if os.path.isfile(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    shutil.copyfile(TEMPLATE_PATH, OUTPUT_FILE)

    book = load_workbook(OUTPUT_FILE)
    writer = pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl')
    writer.book = book
    # Extract all sheet name
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    df.to_excel(writer, 'Sheet1', index=False, header=False, startrow=8, startcol=0)
    writer.save()


def remove_ftp_folder():
    cwd = os.getcwd()
    fy_files = f"{cwd}/Input Files/FTP Files"
    fiscal_folders = [f.path for f in os.scandir(fy_files) if f.is_dir()]
    for folder in fiscal_folders:
        shutil.rmtree(folder)


def remove_input_files():
    cwd = os.getcwd()
    files = [i.path for i in os.scandir(f"{cwd}/Input Files/") if i.is_file()]
    for file in files:
        os.remove(file)


def remove_output_files():
    cwd = os.getcwd()
    files = [i.path for i in os.scandir(f"{cwd}/Output Files/") if i.is_file()]
    for file in files:
        os.remove(file)


def csv_reader(file):
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
            pass

    return df


def processor():
    # Remove existing FTP files.
    remove_ftp_folder()

    # Remove Input Files
    remove_input_files()

    # Remove Output Files
    remove_output_files()

    # Logging Start Time
    logger("Starting Poly Audit Process at - " + str(datetime.datetime.now()))

    # Get FTP Details
    config = RawConfigParser()
    config.read("config.ini")
    HOST = config.get('FTP Credentials', 'HOST')
    PORT = int(config.get('FTP Credentials', 'PORT'))
    USERNAME = config.get('FTP Credentials', 'USERNAME')
    PASSWORD = config.get('FTP Credentials', 'PASSWORD')
    TOTAL_KEYWORDS = config.get('Total Keywords', 'KEYWORDS').split(',')

    # Initialize FTP Module
    ftp_obj = FTP_Loader(host=HOST, user_name=USERNAME, password=PASSWORD, port=PORT)

    # Downloading the files for processing.
    logger("Downloading Config, OBIIE, Model N files.")
    available_files = [file for file in ftp_obj.ftp_getfiles() if file.endswith(".xlsx") or file.endswith(".csv")]
    for file in available_files:
        file = re.findall(r"(?<=:\d{2}\s).+", file)[0]
        cwd = os.getcwd()
        destination = f"{cwd}/Input Files/{file}"
        try:
            ftp_obj.ftp_downloadfile(file, destination)
        except:
            ftp_obj = FTP_Loader(host=HOST, user_name=USERNAME, password=PASSWORD, port=PORT)
            ftp_obj.ftp_downloadfile(file, destination)

    # File Names
    cwd = os.getcwd()
    files = [i.path for i in os.scandir(f"{cwd}/Input Files/") if i.is_file()]
    CONFIG_FILE = [i for i in files if 'Config' in i][0]
    OBIIE_FILE = [i for i in files if ".csv" in i][0]
    MODEL_N_FILE = [i for i in files if i not in [CONFIG_FILE, OBIIE_FILE]][0]

    # Get Partner Details
    df_PartnerMaster = read_audit_configuration(CONFIG_FILE)

    # Initialize Objects
    OBIEEProcessor_obj = OBIEEProcessor(OBIIE_FILE)
    ModelNProcessor_obj = ModelNProcessor(MODEL_N_FILE)

    # Output Dataframe
    dataframe1 = None

    # Start processing each partner
    for index, row in df_PartnerMaster.iterrows():

        logger("\n")
        logger("-" * 50)

        quarter = row[2]
        partner_name = row[0]
        partner_id = re.search(r"(?<=\[).+(?=\])", partner_name).group(0)
        #ftp_file_extension = row[3]
        ftp_file_Sum_Column_Name = row[4]
        ftp_file_FQStartDate = row[5]
        ftp_file_FQEndDate = row[6]
        ftp_file_FQDateColumn = row[7]
        ftp_file_FQDateColumn_DateFormat = row[8]
        ftp_file_DataSheetName = row[9]
        ftp_file_Header_Row = row[10]
        ftp_file_Data_Row = row[11]
        ftp_file_Multupartner = row[12]
        ftp_file_Multupartner_headerColumn = row[13]

        # Remove Time
        ftp_file_FQDateColumn_DateFormat = re.sub(r"\shh.+", "", ftp_file_FQDateColumn_DateFormat, flags=re.IGNORECASE)
        # Fix Month
        ftp_file_FQDateColumn_DateFormat = re.sub(r"M{1,2}", "%m", ftp_file_FQDateColumn_DateFormat, flags=re.IGNORECASE)
        ftp_file_FQDateColumn_DateFormat = re.sub(r"M{3}", "%b", ftp_file_FQDateColumn_DateFormat, flags=re.IGNORECASE)
        # Fix Date
        ftp_file_FQDateColumn_DateFormat = re.sub(r"d{1,2}", "%d", ftp_file_FQDateColumn_DateFormat, flags=re.IGNORECASE)
        # Fix Year
        ftp_file_FQDateColumn_DateFormat = re.sub(r"y{4}", "%Y", ftp_file_FQDateColumn_DateFormat, flags=re.IGNORECASE)
        ftp_file_FQDateColumn_DateFormat = re.sub(r"y{2}", "%y", ftp_file_FQDateColumn_DateFormat, flags=re.IGNORECASE)

        logger(f"Processing Quarter - {quarter} for {partner_name}.")

        # Process OBIEE file
        try:
            temp_df = OBIEEProcessor_obj.processor(partner_name, quarter)
        except Exception as obiee_exception:
            logger(str(obiee_exception))
            continue    # Process next partner

        # Process Model-N
        temp_df = model_n_processor(temp_df, ModelNProcessor_obj)

        # Generate Partner Sum
        temp_df["Partner Submission"] = 0
        temp_df["Out of Scope Dates"] = 0
        for row_index, row_value in enumerate(temp_df.iterrows()):
            try:
                FYQTR = row_value[1]["FYQTR"]
                Submitter_ID = row_value[1]["Submitter Oracle ID"]
                partner_file_id = row_value[1]["Partner Reported File Name"].split('-')[0]
                ftp_source_file_path = f"/{FYQTR}/{Submitter_ID}/{partner_file_id}"
                destination_file_path = "Input Files/FTP Files"
                if FYQTR == '':
                    continue
                logger(f"Processing File ID - {partner_file_id}")

                ftp_obj = FTP_Loader(host=HOST, user_name=USERNAME, password=PASSWORD, port=PORT)
                ftp_file_local_path = ftp_obj.downloadFiles(ftp_source_file_path, destination_file_path)
                ftp_obj.ftp_close()

                file_ext = ftp_file_local_path.split('.')[-1].lower()
                ftp_file_df = None
                if "xlsx" in file_ext or "xls" in file_ext or "xlsm" in file_ext:
                    xls = xlrd.open_workbook(ftp_file_local_path, on_demand=True)
                    if ftp_file_DataSheetName not in xls.sheet_names():
                        ftp_file_DataSheetName = 0
                    ftp_file_df = pd.read_excel(ftp_file_local_path,
                                                sheet_name=ftp_file_DataSheetName,
                                                skiprows=ftp_file_Header_Row-1)
                else:
                    ftp_file_df = csv_reader(ftp_file_local_path)

                if None is ftp_file_df:
                    raise Exception(f"Unable to read file - {ftp_file_local_path}.")

                list_drop_rows = [i for i in range(0, (ftp_file_Data_Row-1-ftp_file_Header_Row))]
                if len(list_drop_rows) > 0:
                    ftp_file_df = ftp_file_df.drop(list_drop_rows)
                # Extracting Date Column name
                QDateColumns = [col for col in ftp_file_df.columns.astype('str') if col.upper().strip() == ftp_file_FQDateColumn.upper().strip()]
                if len(QDateColumns) > 0:
                    ftp_file_FQDateColumn = QDateColumns[0]
                else:
                    raise Exception("Date column not found for Out of Scope Date Calculation.")

                # Convert Out-of-Scope Date column to date type
                if type(list(ftp_file_df.tail()[ftp_file_FQDateColumn])[0]) != datetime.datetime and \
                        not pd.core.dtypes.common.is_datetime_or_timedelta_dtype(ftp_file_df[ftp_file_FQDateColumn]):
                    try:
                        date_series = pd.to_datetime(ftp_file_df[ftp_file_FQDateColumn].str.strip(),
                                                     format=ftp_file_FQDateColumn_DateFormat,
                                                     errors='coerce')
                    except Exception as e:
                        date_series = pd.to_datetime(ftp_file_df[ftp_file_FQDateColumn],
                                                     format=ftp_file_FQDateColumn_DateFormat,
                                                     errors='coerce')

                    if len([i for i in date_series.isna() if not i]) == 0:
                        try:
                            date_series = pd.to_datetime(ftp_file_df[ftp_file_FQDateColumn].str.strip(),
                                                         format="%Y-%m-%d",
                                                         errors='coerce')
                        except Exception as e:
                            raise Exception(f"Date column is not available in proper format. "
                                            f"Please check the values in - '{ftp_file_FQDateColumn}' column.")

                    # Check if Date Series is blank or not.
                    if [i for i in list(date_series) if not pd.isnull(i)].__len__() == 0:
                        raise Exception(f"Date column is not available in proper format. "
                                        f"Please check the values in - '{ftp_file_FQDateColumn}' column.")
                    ftp_file_df[ftp_file_FQDateColumn] = date_series


                # Remove extra rows.
                ftp_file_df = ftp_file_df[ftp_file_df[ftp_file_FQDateColumn].apply(lambda x: isinstance(x, datetime.datetime) or isinstance(x, pd.Timestamp))]

                # Generate 'Partner Submission' and 'Out of Scope' columns
                if None is not ftp_file_df:

                    # Filter out of scope df
                    if ftp_file_Multupartner.upper() == 'YES':
                        if ftp_file_Multupartner_headerColumn in ftp_file_df.columns:
                            if str(partner_id) == '261620':
                                partner_id = 'ITA-FRA-001'
                            ftp_file_df = ftp_file_df[ftp_file_df[ftp_file_Multupartner_headerColumn].astype('string').str.contains(partner_id)]
                            # ftp_file_df = ftp_file_df[ftp_file_df[ftp_file_Multupartner_headerColumn].astype('string')==partner_id]
                        else:
                            logger(f"Multi Partner column - {ftp_file_Multupartner_headerColumn} does not exists in the file.")

                    # Remove rows with blank Quarter Date
                    ftp_file_df = ftp_file_df[ftp_file_df[ftp_file_FQDateColumn].notnull()]

                    # Generate Partner Submission and Out of Scope Sum
                    if ftp_file_Sum_Column_Name in ftp_file_df.columns:
                        Sum_Partner_Submission = sum([math.ceil(round(float(i), 2)) for i in ftp_file_df[ftp_file_Sum_Column_Name] if len(re.findall(r"^-?\d+(\.\d+)?$", str(i).strip())) > 0])
                        if ftp_file_FQDateColumn in ftp_file_df.columns:
                            ftp_file_df = ftp_file_df[
                                (pd.to_datetime(ftp_file_df[ftp_file_FQDateColumn]) < ftp_file_FQStartDate) |
                                (pd.to_datetime(ftp_file_df[ftp_file_FQDateColumn]) > ftp_file_FQEndDate)]
                            Sum_Out_Of_Scope = ftp_file_df[ftp_file_Sum_Column_Name].sum()
                        else:
                            logger(f"Date column - {ftp_file_FQDateColumn} does not exists in the file.")
                            Sum_Out_Of_Scope = 0
                    else:
                        logger(f"Partner Submission column - {ftp_file_Sum_Column_Name} does not exists in the file, so the partner submission is 0.")
                        Sum_Partner_Submission = 0
                        Sum_Out_Of_Scope = 0

                else:
                    Sum_Partner_Submission = 0
                    Sum_Out_Of_Scope = 0

                temp_df.at[row_index, 'Partner Submission'] = Sum_Partner_Submission
                temp_df.at[row_index, 'Out of Scope Dates'] = Sum_Out_Of_Scope

            except Exception as file_exception:
                exception_msg = "Error: " + str(file_exception)
                logger(exception_msg)
                pass

        # Sum up Partner Submission Column
        temp_df.at[temp_df.shape[0] - 1, 'Partner Submission'] = temp_df['Partner Submission'].sum()

        # Sum up out of scope column
        temp_df.at[temp_df.shape[0] - 1, 'Out of Scope Dates'] = temp_df['Out of Scope Dates'].sum()
        # Merge current Partner data with final DF.
        if dataframe1 is None:
            dataframe1 = temp_df
        else:
            frames = [dataframe1, temp_df]
            dataframe1 = pd.concat(frames, sort=False)



    # Adding 2 more columns
    dataframe1["FileID"] = dataframe1["Partner Reported File Name"].apply(lambda x: x.split('-')[0])
    dataframe1["LookUp"] = dataframe1["FileID"] + dataframe1["Submitter Name"]

    cols = dataframe1.columns.tolist()
    cols = [cols[-1]] + [cols[-2]] + cols[:-2]

    dataframe1 = dataframe1[cols]

    # Convert numerical columns from STR to INT
    dataframe1 = dataframe1.astype({'1': int, '2': int, '3': int,
                                    '4': int, '5': int, '6': int,
                                    '7': int, '8': int, '9': int,
                                    '10': int, '11': int, 'FileID': int,
                                    'Submitter Oracle ID': int}, errors="ignore")

    # Adding Grand Total Row
    grand_total_data = ['Grand Total', '', 'Grand Total', '', '', '', '', '']
    total_df = dataframe1[dataframe1['LookUp'].str.contains("Total")]
    sum_cols = [sum([int(i) for i in total_df[str(idx)] if i]) for idx in range(1, 14)]
    remaining_cols = [sum([int(i) for i in total_df[str(idx)] if i]) for idx in list(total_df.columns[21:])]
    grand_total_data = grand_total_data + sum_cols + remaining_cols

    dataframe1.loc[dataframe1.index.max()+1] = grand_total_data

    # Writing Excel
    output_writer(dataframe1)

    # Data Formatter

    fiscal_year = df_PartnerMaster['Current Fiscal Quarter'][0]
    fiscal_year = f"Q{fiscal_year.split('Q')[1]} 20{fiscal_year.split('Q')[0].replace('FY','')}"
    obj = OutputFormatter("Output Files/Audit Process Report.xlsx")
    obj.processor(fiscal_year)
    obj.file_saver()
    obj.data_grouper(dataframe1)

    # Remove existing FTP files.
    remove_ftp_folder()

    # Remove Input Files
    remove_input_files()


def logger(txt):
    print(txt)
    if os.path.isfile("Output Files/Process_Log.txt"):
        write_mode = 'a'
    else:
        write_mode = 'w'

    with open("Output Files/Process_Log.txt", write_mode) as file_obj:
        file_obj.write("\n"+str(txt))


if __name__ == '__main__':
    try:
        processor()
    except Exception as e:
        print("Exception occurred: " + str(e))
        logger(str(e))
        pass
