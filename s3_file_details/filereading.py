import glob
import  ast
import pymysql
from django.shortcuts import HttpResponse
from rest_framework.parsers import MultiPartParser, FormParser, FileUploadParser
from rest_framework.response import Response
from rest_framework.views import APIView

from .Dynamic_Rule_Script import *
from .models import *
from .TG_purchase_validation import *
from .TG_Sales_validation import *
from .serializers import *
from .tg_sales2 import *

global connection
from pathlib import Path
global data3
global BASE_DIR
from pyxlsb import open_workbook
# from xlrd import open_workbook, XLRDError
from pyexcelerate import Workbook

class Filere(object):
    def fileread(self,fileExtension, sheetname, filename, header):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(BASE_DIR, 'BASE_DIR')
        d = BASE_DIR + '/media/'
        d2 = BASE_DIR + '/Merge/'
        d3 = BASE_DIR + '/client_compute/'
        d4 = BASE_DIR + '/media/Master_Files/'
        print(d, "d")
        count = 0
        global pathhh,data3
        data3=[]
        path = [d, d2, d3, d4]
        for i in path:
            if glob.glob(i + filename) != 0:
                print(i)
                check = glob.glob(i + filename)
                print(len(check), "check COUNT OF FILE")
                print("glob")
                pathhh = i + filename
                print(pathhh, 'yes')
                count = count + 1
                print(count, "count")
                if len(check) == 1:
                    if (fileExtension == 'xlsx') | (fileExtension == 'xls') | (fileExtension == 'XLSX'):
                        try:
                            df_data = pd.read_excel(pathhh, sheet_name=sheetname, header=int(header) - 1, sort=False,na_filter=False)
                            print(type(df_data))
                            print("corrupt in")
                            columns = list(df_data.columns)
                            print(columns, "col")
                            colu = all(isinstance(n, str) for n in columns)
                            print(colu, "colucolucolu")
                            for i in columns:
                                data3.append(i)
                            print(data3,"data3")
                            if (colu == True):
                                print("ifAWSEDFG")
                            else:
                                df_data = 'Incorrect Row Number'
                            return df_data
                        except Exception as e:
                            print(e)
                            if str(e).startswith('No'):
                                e = 'Invalid Sheetname'
                                return e
                            elif str(e).startswith('Unsupported'):
                                e = 'File is corrupted/Unable to read'
                                return e
                            elif str(e).startswith('Passed'):
                                e = 'At Given Row Number Data Not Present'
                                return e
                            else:
                                return str(e)

                    elif len(check) == 0:
                        df_data = 'File Nothjasnflksdfn '
                    elif fileExtension == 'xlsb':
                        print("m at xlsb")
                        try:
                            df_data = []
                            data3 = []
                            print("ASFDFFSDFSDF")
                            print(pathhh, "pathhh")
                            xls_file = open_workbook(pathhh)
                            print("ASFDasdfghjdsdfghjFFSDFSDF")
                            with xls_file.get_sheet(sheetname) as sheet:
                                for row in sheet.rows():
                                    df_data.append([item.v for item in row])
                                df_data = pd.DataFrame(df_data[int(header):], columns=df_data[int(header) - 1])
                                columns = list(df_data.columns)
                                print(columns, "col")
                                colu = all(isinstance(n, str) for n in columns)
                                print(colu, "colucolucolu")
                                for i in columns:
                                    data3.append(i)
                                print(data3,"ddddddddddd")
                                if (colu == True):
                                    print("ifAWSEDFG")
                                else:
                                    df_data = 'Incorrect Row Number'
                                return df_data

                        except Exception as e:
                            print(e, "EEEEEEEEEEE")
                            if str(e).endswith('is not in list'):
                                e = 'Invalid Sheetname'
                                return e
                            elif str(e).endswith('index out of range'):
                                e = 'At Given Row Number Data Not Present'
                                return e
                            elif str(e).startswith('File is not a zip'):
                                e = 'File is corrupted/Unable to read'
                                return e
                            elif str(e).startswith('Passed'):
                                e = 'At Given Row Number Data Not Present'
                                return e
                            else:
                                return str(e)
                    elif fileExtension == 'csv':
                        try:
                            data3 = []
                            df_data = pd.read_csv(pathhh, encoding="ISO-8859-1", na_filter=False)
                            print("M IN READ CSV")
                            print(len(df_data))
                            print("corrupt in")
                            columns = list(df_data.columns)
                            print(columns, "col")
                            colu = all(isinstance(n, str) for n in columns)
                            print(colu, "colucolucolu")
                            for i in columns:
                                data3.append(i)
                            if (colu == True):
                                print("ifAWSEDFG")
                            else:
                                df_data = 'Incorrect Row Number'
                            return df_data

                        except Exception as e:
                            print(e)
                            if str(e).startswith('No'):
                                e = 'Invalid Sheetname'
                                return e
                            elif str(e).startswith('Error tokenizing data'):
                                e = 'File is corrupted/Unable to read'
                                return e
                            elif str(e).startswith('Passed'):
                                e = 'At Given Row Number Data Not Present'
                                return e
                            else:
                                return str(e)
    def newfunc(self):
        print(data3,"acesssss")
        return data3

