import pandas as pd
from OBIEEProcessor import OBIEEProcessor
from ModelNProcessor import ModelNProcessor
from FTP_Loader import FTP_Loader
import re
from configparser import RawConfigParser
import os
import shutil
from openpyxl import load_workbook
import codecs
import datetime


def csv_reader(file):
    list_deli = [',', '\t', '|', ':',';']
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
            break
        except Exception as e:
            pass

    return df


print("")

df = pd.read_excel(r"C:\Users\v-isbind\Desktop\Temp\poly POS_2020.12.07.xlsx")
ftp_file_df = csv_reader(r"C:\Users\v-isbind\OneDrive - Microsoft\Ishmeet Bindra\Learning\Contributed Projects\FLWork\Bomisco\Automation Tasks\3. New Work\Audit Process\Code\Input Files\SellOutData20201207.txt")