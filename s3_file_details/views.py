from rest_framework.views import APIView
from rest_framework import status
from .serializers import *
import pymysql
import pandas as pd
import json
import os
from itertools import zip_longest
import re
from datetime import datetime
from rest_framework.response import Response
from .models import *
import boto3
from time import strftime
import boto3
import io
from django.shortcuts import HttpResponse
from pathlib import Path
from boto3 import client
import ast
from pathlib import Path
from rest_framework.parsers import MultiPartParser, FormParser, FileUploadParser
import botocore
from io import StringIO
from datetime import datetime
# import datetime
import mysql.connector
from .TG_Sales_validation import *
from .Dynamic_Rule_Script import *
from .TG_purchase_validation import *
import xlrd
import glob
#from .Sales_validation import *
#from .purchase_validation import *
global connection
from pathlib import Path
import time
global BASE_DIR
from pyxlsb import open_workbook
from pyexcelerate import Workbook
from .filereading import *

class StartView(APIView):
    def post(self,request):
        try:
            Company_Id = request.data['Company_Id']
            type=request.data['File_Type']
            month=request.data['month2']
            financial_year=request.data['Financial_Year']
            print(financial_year)
            status2= ['Deleted', 'Uploaded']
            sql_query=list(filerecord.objects.filter(Company_Id=Company_Id,type=type,month2=month,financial_year=financial_year).exclude(Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
            # print(sql_query)
            return Response(({"status": sql_query}),status=status.HTTP_201_CREATED)
        except Exception as e:
            print(e)
            return Response(({"status": "Something Went Wrong,Please Try Again"}),status=status.HTTP_201_CREATED)

class DeleteView(APIView):
    def post(self,request):
        Company_Id = request.data['Company_Id']
        type=request.data['File_Type']
        month=request.data['month']
        financial_year=request.data['Financial_Year']
        delete='Deleted'
        sql_query=list(filerecord.objects.filter(Company_Id=Company_Id,type=type,month2=month,financial_year=financial_year,status="Deleted").order_by('-id').values())
        return Response(({"status": sql_query}), status=status.HTTP_201_CREATED)

class Processed(APIView):
    def post(self,request):
        Company_Id = request.data['Company_Id']
        type=request.data['File_Type']
        month=request.data['month']
        financial_year=request.data['Financial_Year']
        uploaded='Uploaded'
        sql_query=list(filerecord.objects.filter(Company_Id=Company_Id,type=type,month2=month,financial_year=financial_year,Action_Status=uploaded).order_by('-id').values())
        # print(sql_query,"processsed")
        return Response(({"status":sql_query,"statuss":"success"}), status=status.HTTP_201_CREATED)

class Filter(APIView):
    def post(self,request):
        Company_Id = request.data['Company_Id']
        print(Company_Id)
        type=request.data['File_Type']
        print(type)
        month2=request.data['month2']
        print(month2)
        financial_year=request.data['Financial_Year']
        print(financial_year)
        if type=="GSTR1-Sales" or type=="GSTR2-Purchase" or type=="General":
            sql_query=list(filerecord.objects.filter(Company_Id=Company_Id,type=type,month2=month2,financial_year=financial_year).exclude(Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
            # print(sql_query,"filter")
            return Response(({"status":sql_query}),status=status.HTTP_201_CREATED)

class UploadView2(APIView):
    parser_classes = (MultiPartParser, FormParser, FileUploadParser)
    def post(self, request):
        datuu=request.data
        print(datuu,"datuu")
        upload_data =UploadSerializer(data=request.data)
        if upload_data.is_valid():
            upload_data.save()
            uppu = upload_data.data
            print(uppu, "uppuuppu")
            print(upload_data.data['type'], "typeeeeeee")
            time = upload_data.data['Timestamp']
            id = upload_data.data['id']
            Company_Id = upload_data.data['Company_Id']
            month = str(upload_data.data['month2'])
            Action_Status = upload_data.data['Action_Status']
            filetype = upload_data.data['type']
            file_name1 = os.path.basename(upload_data.data['file'])
            fileExtension = file_name1.split(os.extsep)[-1]
            filename2 = os.path.splitext(file_name1)[0]
            if (filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'xlsx') or (
                    filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'xls') or (
                    filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'XLSX') or (
                    filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'xlsb'):
                datta = time.replace("T", "_")
                s3_file_path = filename2 + "_" + datta + '.' + fileExtension
                print(s3_file_path, "s3_file_path")
                see2 = filerecord.objects.filter(id=id, filename_Timestamp='file')
                for i in see2:
                    i.raw_file_path = file_name1
                    i.RawFileRef_Id = id
                    i.filename_Timestamp = s3_file_path
                    i.save()
                BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                print(BASE_DIR, 'BASE_DIR')
                file_size = os.path.getsize(BASE_DIR + '/media/' + file_name1)
                file_size2 = str(file_size) + " kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=filetype, month2=month).exclude(
                    Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
                return Response(({"status": sql_query, "status2": "Success", "reason": "File Successfully Uploaded"}),status=status.HTTP_201_CREATED)
            elif (filetype == 'GSTR2-Purchase' or filetype == 'GSTR1-Sales') and (fileExtension == 'csv'):
                print("m here for csv")
                datta = time.replace("T", "_")
                s3_file_path = filename2 + "_" + datta + '.' + fileExtension
                print(s3_file_path, "s3_file_path")
                see2 = filerecord.objects.filter(id=id, filename_Timestamp='file')
                for i in see2:
                    i.raw_file_path = file_name1
                    i.RawFileRef_Id = id
                    i.filename_Timestamp = s3_file_path
                    i.save()
                BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                print(BASE_DIR, 'BASE_DIR')
                file_size = os.path.getsize(BASE_DIR + '/media/' + file_name1)
                file_size2 = str(file_size) + " kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=filetype, month2=month).exclude(
                    Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
                return Response(({"status": sql_query, "status2": "Success", "reason": "File Successfully Uploaded"}),
                                status=status.HTTP_201_CREATED)
            elif filetype == 'General':
                datta = time.replace("T", "_")
                s3_file_path = filename2 + "_" + datta + '.' + fileExtension
                print(s3_file_path, "s3_file_path")
                see2 = filerecord.objects.filter(id=id, filename_Timestamp='file')
                for i in see2:
                    i.raw_file_path = file_name1
                    i.RawFileRef_Id = id
                    i.filename_Timestamp = s3_file_path
                    i.save()
                BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                print(BASE_DIR, 'BASE_DIR')
                file_size = os.path.getsize(BASE_DIR + '/media/' + file_name1)
                file_size2 = str(file_size) + " kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = list(filerecord.objects.filter(Company_Id=Company_Id, type=filetype, month2=month).exclude(
                    Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
                return Response(({"status": sql_query, "status2": "Success", "reason": "File Successfully Uploaded"}),
                                status=status.HTTP_201_CREATED)
            else:
                if fileExtension != 'xlsx' or fileExtension != 'csv' or fileExtension != 'xlsb':
                    deleting = filerecord.objects.filter(id=id).delete()
                    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    print(BASE_DIR, 'BASE_DIR')
                    os.remove(BASE_DIR + '/media/' + file_name1)
                    return HttpResponse(json.dumps({"status2": "failed", "reason": "Only xlsx,csv Files Allowed"}),
                                        status=status.HTTP_201_CREATED)

class Delete_files(APIView):
    def post(self, request):
        del_data=request.data['list']
        print(del_data)
        Company_Id=request.data['Company_Id']
        print(Company_Id)
        purpose= request.data['File_Type']
        print(purpose)
        time=str(datetime.now())
        ls=[]
        for i in del_data:
            ls.append(i.get("id"))
        data_del=tuple(ls)
        print(data_del)
        for i in ls:
            deleting=filerecord.objects.filter(id=i)
            for i in deleting:
                i.status='Deleted'
                i.Deleted_at=time
                i.Action_Status='Deleted'
                i.save()
        return HttpResponse(json.dumps({"status": "success","response":"Files Deleted"}), status=status.HTTP_201_CREATED)

class hard_query(APIView):
    def post(self,request):
        sheet_row_merge=request.data
        print(sheet_row_merge,"merge data")
        merge2=[]
        statuss=[]
        for i in sheet_row_merge:
            merge2.append(i['actionStatus'])
        res = all(ele == merge2[0] for ele in merge2)
        for i in range(0,len(merge2)):
            statuss.append('New')
        print(merge2,"merge2")
        if merge2!=statuss:
            return HttpResponse(json.dumps({"status": "failed","response":"You can Merge New Status Files Only"}), status=status.HTTP_201_CREATED)
        financial_year=sheet_row_merge[0]
        financial_year2=financial_year['financial_year']
        type=sheet_row_merge[0]
        type2=type['type']
        Company_Id=sheet_row_merge[0]
        Company_Id2=Company_Id['companyId']
        statuss=sheet_row_merge[0]
        actionStatus=statuss['actionStatus']
        statuss2=sheet_row_merge[1]
        actionStatus2=statuss['actionStatus']
        month=sheet_row_merge[0]
        month2=month['month']
        month3=sheet_row_merge[0]
        month4=month['month2']
        file=sheet_row_merge[0]
        file2=file['fileName']
        print(file2,"file22222222222")
        ls = len(sheet_row_merge)
        file_id = []
        header = []
        sheet = []
        for i in range(ls):
            a = sheet_row_merge[i]['id']
            b = sheet_row_merge[i]['Sheet']
            c = sheet_row_merge[i]['Header']
            file_id.append(a)
            sheet.append(b)
            header.append(c)
        for i in range(0,len(file_id)):
             see3 = filerecord.objects.filter(id=file_id[i]).update(sheet_name=sheet[i],row_no=header[i])
        combined_df=pd.DataFrame()
        for i in file_id:
            print(i,"iiiiiiiiiiiii")
            file = list(filerecord.objects.filter(id=i).values('raw_file_path'))
            print(file,"fileeeeeeeeeeeeeeee")
            filenamee=file[0]
            fileExtension = filenamee['raw_file_path'].split(os.extsep)[-1]
            if fileExtension=='xlsx' or fileExtension=='csv' or fileExtension=='xlsb' or fileExtension=="xls" or 'XLSX':
                sql_query=list(filerecord.objects.filter(id=i).values('filename','sheet_name','row_no',))
                print(sql_query,"adsssddsd")
                file_name=sql_query[0]['filename']
                fileExtension = file_name.split(os.extsep)[-1]
                file_sheet=sql_query[0]['sheet_name']
                file_row=int(sql_query[0]['row_no'])
                d=Filere()
                df_data = d.fileread(fileExtension,file_sheet,file_name,file_row)
                print(df_data,"df_data")
                s = d.newfunc()
                print(s,"sssssssssssssss")
                df45 = df_data
                if (isinstance(df45,str)):
                    return HttpResponse(json.dumps({"status": "failed","response":df45}), status=status.HTTP_201_CREATED)
                else:
                    combined_df=combined_df.append(df45).reset_index(drop=True)
        print(combined_df,"here is output")
        output=combined_df.columns
        print(len(output))
        dataaaa=set(s)&set(output)
        print(len(dataaaa))
        print("heehheehehe")
        if len(dataaaa)!=len(output):
            return HttpResponse(json.dumps({"status": "failed","response":"File Headers Should Be Same"}), status=status.HTTP_201_CREATED)
        batch_id=''
        for j in file_id:
            batch_id+=str(j)
        time = str(datetime.now())
        time2 = time.replace(':', '_')
        name='Merge_'+batch_id+'_'+time
        nn=name.replace(':','_')
        nn2=nn.replace('.','_')
        nn3=nn2+'.csv'
        typee=sheet_row_merge[0]['type']
        print(typee,"typee")
        merg_name = "Merge_File_"+typee+'_'+time2+".csv"
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(BASE_DIR, 'BASE_DIR')
        combined_df.to_csv(BASE_DIR+"/Merge/"+merg_name,index=False)
        file_size=os.path.getsize(BASE_DIR+"/Merge/"+merg_name)
        file_size2=str(file_size)+" kb"
        batch_ref='Batch_id_'+batch_id
        creating=filerecord.objects.create(file=file2,filename=merg_name,filetype='.csv',filename_Timestamp=merg_name,RawFileRef_Id='NA',Action_Status='Merge',row_no='NA',sheet_name='NA',HQId='NA',sub_state='NA',stock_id='NA',Branch_id='NA',Merge_by='NA',Merge_Batch_Id='NA',Deleted_by='NA',Archived_by='NA',processed_by='NA',raw_file_path=merg_name,processed_file_path='NA',error_file_path='NA',control_summary_path='NA',Merge_file_path='NA',Company_Id=Company_Id2,type=type2,financial_year=financial_year2,month=month2,file_size=file_size2,Merge_Reference=batch_ref,month2=month4,Row_Size='NA',Column_Size='na',Unique_Invoices='na',Total_Tax_Value='na',Total_Taxable_Vale='na')
        see3 = filerecord.objects.filter(filename=merg_name)
        for i in see3:
            i.Uploadedtimestamp=time
            i.Uploaded_by=time
            i.Timestamp=time
            i.row_no='1'
            i.sheet_name=nn2
            i.save()
        return HttpResponse(json.dumps({"status": "success","response":"Your Files Successfully Mergerd"}), status=status.HTTP_201_CREATED)

class Mapping_file_sheets(APIView):
    def post(self, request):
        Company_Id = request.data['Company_Id']
        merge_data=request.data['list']
        print(merge_data)
        type=merge_data.get("type")
        filename=merge_data.get("file")
        filename2=merge_data.get("filename")
        print(filename,"this is filename")
        file=[filename2]
        print(file,"filename in list")
        actionStatus=merge_data.get("Action_Status")
        fileExtension = filename2.split(os.extsep)[-1]
        print(fileExtension,"fileExtension")
        print("111")
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(BASE_DIR, 'BASE_DIR')
        if actionStatus=='New' and (type=='GSTR1-Sales' or type=='GSTR2-Purchase') or actionStatus=='Compute' and (type=='GSTR1-Sales' or type=='GSTR2-Purchase') or actionStatus=='Merge' and (type=='GSTR1-Sales' or type=='GSTR2-Purchase'):
            print("something")
            print(fileExtension)
            try:
                if actionStatus=='New' and (fileExtension =='xlsx' or fileExtension== 'xls' or fileExtension== 'XLSX'):
                    xl = pd.ExcelFile(BASE_DIR+'/media/'+filename)
                    print("in media")
                    sheetnames=xl.sheet_names
                    print("Sheet Name media:== ", sheetnames)
                    return HttpResponse(json.dumps({"status":"success","sheet_names": sheetnames}), status=status.HTTP_201_CREATED)

                if actionStatus=='Compute' and (fileExtension =='xlsx' or fileExtension== 'xls' or fileExtension== 'XLSX'):
                    print("in compute")
                    xl = pd.ExcelFile(BASE_DIR+'/client_compute/'+file[0])
                    sheetnames=xl.sheet_names
                    print("Sheet Name Compute:== ", sheetnames)
                    return HttpResponse(json.dumps({"status":"success","sheet_names": sheetnames}), status=status.HTTP_201_CREATED)

                elif actionStatus=='New' and fileExtension =='xlsb':
                    wb = open_workbook(BASE_DIR+'/media/'+filename)
                    sheetnames=wb.sheets
                    return HttpResponse(json.dumps({"status":"success","sheet_names": sheetnames}), status=status.HTTP_201_CREATED)

                elif fileExtension =='csv':
                    print("csvvvv")
                    print("222")
                    return HttpResponse(json.dumps({"status":"success","sheet_names": file}), status=status.HTTP_201_CREATED)

            except Exception as e:
                print(e,"eeeeee")
                print(e)
                return HttpResponse(json.dumps({"status":"failed","response": "Unsupported format, or corrupt file"}), status=status.HTTP_201_CREATED)

class file_details_sheet_row(APIView):
    def post(self, request):
        data3=request.data
        print(data3,"data3")
        file_prcess=request.data['file_process_type']
        Template_Name=data3['Template_Name']
        comp_id=data3['Company_Id']
        Type=data3['Type']
        queryset_1 =list(Mapping.objects.filter(Company_Id=comp_id,Type=Type).values('Template_Name'))
        for i in queryset_1:
            if i['Template_Name']==Template_Name:
                return HttpResponse(json.dumps({"status2":"exists","status": "Template Name already exists"}))
        file_sheet = data3['sheet_name']
        file_row = int(data3['row_no'])
        Action_Status = data3['Action_Status']
        file_filename = data3['filename']
        fileExtension = file_filename.split(os.extsep)[-1]
        print(fileExtension, "fileExtension")
        filenameee = data3['filename_Timestamp']
        print(filenameee, "filenameee")
        see3 = Mapping.objects.filter(filename_Timestamp=filenameee).update(Company_Id=comp_id)
        print("updateddddd")
        if Action_Status == 'New' or Action_Status == 'Compute' or Action_Status == 'Merge':
            d=Filere()
            df_data =d.fileread(fileExtension, file_sheet, file_filename, file_row)
            print(df_data, "df_data")
            if (isinstance(df_data, str)):
                return HttpResponse(json.dumps({"status": "failed", "response": df_data}),status=status.HTTP_201_CREATED)
            else:
                columns = list(df_data.columns)
                global template_details
                template_details=request.data
                print(template_details,"template_details")
                if file_prcess=='TG':
                    return HttpResponse(json.dumps({"columns": columns, "status": "success"}),status=status.HTTP_201_CREATED)
                else:
                    sheet_row_data = Mapping_serializer(data=request.data)
                    if sheet_row_data.is_valid():
                        sheet_row_data.save()
                        return HttpResponse(json.dumps({"columns": columns, "status": "success"}),status=status.HTTP_201_CREATED)

class system_col(APIView):
    def post(self, request):
        try:
            base = request.data['invoiceType']
            type1 = request.data['type']
            print("ggggg")
            connection = pymysql.connect(host='15.206.93.178', user="taxgenie", password="taxgenie*#8102*$",database="taxgenie_efilling")
            if len(base) == 1:
                invoice_type = 'where invoiceType = "'+base[0]+'" and type = "'+type1+'"'
                print(invoice_type,"invoice_type1")
            else:
                invoice_type = 'where invoiceType in '+str(tuple(base))+' and type = "'+type1+'"'

            sysCol = pd.read_sql("SELECT DISTINCT `field`, `required` FROM `system col_s3` "+invoice_type, connection)
            print(sysCol,"sysCol")
            connection.close()
            sys_col = sysCol.to_json(orient='records')
            return HttpResponse(json.dumps({"status":"success","response":sys_col}),status=status.HTTP_201_CREATED)
        except Exception as e:
            return HttpResponse(json.dumps({"status": "failed", "response": "No Invoice Type Selected"}), status=status.HTTP_201_CREATED)

class Col_header(APIView):
    def post(self, request):
        temp_name = request.data['Template_Name']
        column_header = request.data['column_header']
        Company_Id = request.data['Company_Id']
        see3 = Mapping.objects.create(filename=template_details['filename'] ,filename_Timestamp=template_details['filename_Timestamp'],Type=template_details['Type'] ,Company_Id=template_details['Company_Id'] ,sheet_name=template_details['sheet_name'] ,row_no=template_details['row_no'] ,Template_Name=template_details['Template_Name'],Action_Status=template_details['Action_Status'],column_header=column_header)
        return HttpResponse(json.dumps({"status": "success"}), status=status.HTTP_201_CREATED)

class choose_template(APIView):
    def post(self, request):
        selection=request.data['number']
        print(selection)
        Company_Id = request.data['Company_Id']
        print(Company_Id,"COMPPP")
        Action_Status = request.data['Action_Status']
        print(Action_Status)
        type = request.data['Type']
        print(type)
        if (selection=='Tg') and (Action_Status == "New" or Action_Status == "Merge" or Action_Status == "Compute") :
            sql_query=list(Mapping.objects.filter(Company_Id=str(Company_Id),Type=type,column_header__isnull=False).order_by('-id').values('id','Template_Name'))
            print(sql_query,"datatatataa")#
            return HttpResponse(json.dumps({"status":"success","reason":sql_query}),status=status.HTTP_201_CREATED)
        elif ( selection=='Comp') and (Action_Status == "New" or Action_Status == "Merge") :
            sql_query=list(Mapping.objects.filter(Company_Id=Company_Id,Type=type,column_header__isnull=True).order_by('-id').values('id','Template_Name'))
            print(sql_query,"datatatataa")#
            return HttpResponse(json.dumps({"status": "success", "reason": sql_query}),status=status.HTTP_201_CREATED)
        else:
            return HttpResponse(json.dumps({"status": "failed", "reason": "You Have Already Processed File for this Process"}),status=status.HTTP_201_CREATED)

class Tg_File_Processing(APIView):
    def post(self, request):
        try:
            Action_Status=request.data['actionstatus']
            print(Action_Status,"actionstatus")
            temp_name = request.data['Template_Name']
            temp_name2=temp_name['Template_Name']
            raw_path = request.data['raw_file_path']
            month2 = request.data['month2']
            month = request.data['month']
            year = request.data['financial_year']
            tg = request.data['Tg_Data']
            print(tg,"tggggggggggg")
            Type = request.data['Type']
            print(Type,"TypeType")
            Company_Id = request.data['Company_Id']
            file = request.data['filename']
            print(file,"processing file")
            fileExtension = file.split(os.extsep)[-1]
            print(fileExtension,"fileExtension is here")
            gstin=request.data['GSTIN']
            pan_num = gstin[2:-3]
            print("This is pan +++++++++++++++++++++++++++++++++++++++++++++++",pan_num)
            if Action_Status=='TG-FILE':
                return HttpResponse(json.dumps({"status":"failed","reason":"You Have Already Computed File"}),status=status.HTTP_201_CREATED)
            elif Action_Status=='New' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase') or Action_Status=='Compute' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase')  or  Action_Status=='Merge' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase'):
                sql_query=list(Mapping.objects.filter(Company_Id=Company_Id,Type=Type,Template_Name=temp_name2).values())
                row=sql_query[0]['row_no']
                sheet=sql_query[0]['sheet_name']
                map1=list(Mapping.objects.filter(Company_Id=Company_Id,Type=Type,Template_Name=temp_name2).values())
                print(map1)
                temp_id=map1[0]['id']
                map2=map1[0]['column_header']
                print(map2)
                data = json.loads(map2)
                dt = {}
                data1 = eval(data)
                for d in data1:
                    dt.update(d)
                d=Filere()
                df_data =d.fileread(fileExtension,sheet,file,row)
                if (isinstance(df_data,str)):
                    return HttpResponse(json.dumps({"status": "failed","reason":df_data}), status=status.HTTP_201_CREATED)
                else:
                    statuss='TG-FILE'
                    print("m in tg")
                    df3 = df_data.rename(columns=lambda x: x.strip() if (x != None) else x)
                    df3 = df3.applymap(lambda x: x.strip() if type(x) == str else x)
                    row_col_size = df3.shape
                    print(row_col_size, "row_col_size")
                    df3['M_id']=df3.index+1
                    stage_tb = pd.DataFrame()
                    for user,sys in dt.items():
                        stage_tb[sys]=df3[user]
                    stage_tb['M_id']=df3['M_id']
                    stage_tb['M_id']
                    print(stage_tb.columns,"stage_tb")
                    print(Type,"stage_tbstage_tb")
                    if  Type=='GSTR1-Sales' and (Action_Status=='New' or Action_Status=='Compute'):
                        print("GSTR1-SalesGSTR1-Sales")
                        tg=TG_validation()
                        print("for process")
                        output=tg.Sales(stage_tb,pan_num)
                        print(output.columns,"for upload")
                    elif Type=='GSTR2-Purchase' and (Action_Status=='New' or Action_Status=='Compute'):
                        print("GSTR2-PurchaseGSTR2-Purchase")
                        tg=TG_validation_pr()
                        output=tg.Purchase(stage_tb,pan_num)
                output[['Taxable Value','Invoice Value','Igst Amount','Sgst Amount','Cgst Amount']]=output[['Taxable Value','Invoice Value','Igst Amount','Sgst Amount','Cgst Amount']].replace("na",0).astype(str).convert_objects(convert_numeric=True).fillna(0).astype(float)
                print(output.columns,"output")
                output=pd.merge(left=df3, right=output[['M_id','Status','reason']], on=['M_id'], how='left')
                print("there")
                del output['M_id']
                time = str(datetime.now())
                filename_wout_ext= Path(raw_path).stem
                name=str(filename_wout_ext)+'_'+time
                nn=name.replace(':','_')
                nn2=nn.replace('.','_')
                nn3=nn2+'.xlsx'
                BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                print(BASE_DIR, 'BASE_DIR')
                for i in list(output.select_dtypes(include=['datetime64'])):
                    output[i] = output[i].dt.strftime("%d-%m-%Y")
                data=[output.columns.tolist(), ] + output.fillna('').values.tolist()
                wb = Workbook()
                wb.new_sheet(sheet_name='Sheet1', data=data)
                wb.save(BASE_DIR+"/TG-FILE/"+nn3)
                Fail = output.loc[output['reason']!='']
                for i in list(Fail.select_dtypes(include=['datetime64'])):
                    Fail[i] = Fail[i].dt.strftime("%d-%m-%Y")
                data = [Fail.columns.tolist(), ] + Fail.fillna('').values.tolist()
                wb = Workbook()
                wb.new_sheet(sheet_name='Sheet1', data=data)
                wb.save(BASE_DIR+"/Error_files/"+nn3)
                file_size=os.path.getsize(BASE_DIR+"/TG-FILE/"+nn3)
                file_size2=str(file_size)+"kb"
                CREATING = filerecord.objects.create(file=nn3,filename=nn3,filetype='.xlsx',filename_Timestamp=nn3,RawFileRef_Id=tg[0]['id'],Action_Status='TG-FILE',row_no='NA',sheet_name='NA',HQId='NA',sub_state='NA',stock_id='NA',Branch_id='NA',Merge_by='NA',Merge_Batch_Id='NA',Deleted_by='NA',Archived_by='NA',processed_by='NA',raw_file_path=raw_path,processed_file_path='NA',error_file_path='NA',control_summary_path='NA',Merge_file_path='NA',Company_Id=Company_Id,type=Type,financial_year=year,month= month, file_size=file_size2,Merge_Reference='-',GSTIN=gstin,template_id=temp_id,status=statuss,month2=month2,Row_Size=row_col_size[0]-1,Column_Size=row_col_size[1],Unique_Invoices='na',Total_Tax_Value='na',Total_Taxable_Vale='na')
                see3 = filerecord.objects.filter(filename=nn3)
                for i in see3:
                    i.Uploadedtimestamp = time
                    i.Uploaded_by = time
                    i.Timestamp = time
                    i.save()
                return HttpResponse(json.dumps({"status":"success","reason":"Your File Successfully Processed"}),status=status.HTTP_201_CREATED)
        except KeyError as e:
            return HttpResponse(json.dumps({"status":"failed","reason":str(e) + " Columns  not present in File columns"}),status=status.HTTP_201_CREATED)

class client_file_processing(APIView):
    def post(self, request):
        temp_name = request.data['Template_Name']
        print(temp_name)
        client = request.data['Client_Data']
        client2=client[0]
        temp_name2=temp_name['Template_Name']
        raw_path = request.data['raw_file_path']
        year = request.data['financial_year']
        month = request.data['month']
        month2 = request.data['month2']
        GSTIN=request.data['GSTIN']
        Type = request.data['Type']
        Company_Id = request.data['Company_Id']
        file=request.data['file']
        actionstatus=request.data['actionstatus']
        fileExtension = client2['filename'].split(os.extsep)[-1]
        if actionstatus=='New' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase') or actionstatus=='Merge' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase'):
            sql_query=list(Mapping.objects.filter(Company_Id=Company_Id,Type=Type,Template_Name=temp_name2).exclude(Type='').values())
            print(sql_query,"sql_query")
            row = sql_query[0]['row_no']
            sheet = sql_query[0]['sheet_name']
            statuss='Compute'
            d=Filere()
            df_data =d.fileread(fileExtension,sheet,client2['filename'],row)
            see3 = TgComparisionandarithmeticandfuction2.objects.filter(template_name=temp_name2).update(companyid=Company_Id)
            if (isinstance(df_data,str)):
                return HttpResponse(json.dumps({"status": "failed","reason":df_data}), status=status.HTTP_201_CREATED)
            else:
                try:
                    df2=df_data.rename(columns=lambda x: x.strip())
                    df2= df2.applymap(lambda x: x.strip() if type(x) == str else x)
                    row_col_size=df2.shape
                    print(row_col_size,"row_col_size")
                    #---------------------------------TypeCAST--------------------------------
                    df2.replace(['NA', 'na'], 'Na', inplace=True)

                    ls2 = df2.columns.tolist()
                    # for i in ls2:
                    #     e = df2[i].dtype.name
                    #     if e != 'datetime64[ns]':
                    #         df2[i] = df2[i].apply(pd.to_numeric, errors='ignore')
                    df31 = df2.select_dtypes(exclude=['object'])
                    df41 = df2.select_dtypes(include=['object']).apply(pd.to_numeric, errors='ignore')
                    df2 = pd.concat([df31, df41], axis=1)
                    df2 = df2[ls2]

                    print("m in else")
                    A=TgComparisionandarithmeticandfuction2.objects.filter(companyid=Company_Id,template_name=temp_name2).values()
                    print(A,"AAAAA")
                    rule=pd.DataFrame(A)
                    print(rule,"rulerulerulerule")
                    print(rule.columns,"ruleeee")
                    #----------------------ORIGINAL DATA WITH OUTPUT
                    df2['M_id'] = (df2.index + 1)
                    df_raw = df2.copy()
                    df = purchase_engine(df2, rule)
                    size_raw = len(df_raw.columns)
                    size_raw = (size_raw - 1)
                    df=df.iloc[:, size_raw:]
                    output = pd.merge(left=df_raw, right=df,on=['M_id'], how='left').reset_index(drop=True)
                    del output['M_id']
                    time = str(datetime.now())
                    filename_wout_ext= Path(file).stem
                    name=filename_wout_ext+'_'+time
                    nn=name.replace(':','_')
                    nn2=nn.replace('.','_')
                    time = str(datetime.now())
                    nn3=nn2+'.xlsx'
                    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    print(BASE_DIR, 'BASE_DIR')
                    #output = "\t" + output.fillna('').astype(str)
                    for i in list(output.select_dtypes(include=['datetime64'])):
                        output[i] = output[i].dt.strftime("%d-%m-%Y")
                    data = [output.columns.tolist(), ] + output.fillna('').values.tolist()
                    wb = Workbook()
                    wb.new_sheet(sheet_name='Sheet1', data=data)
                    wb.save(BASE_DIR+"/client_compute/"+nn3)
                    #output.to_excel(BASE_DIR+"/client_compute/"+nn3,index=False)
                    file_size=os.path.getsize(BASE_DIR+"/client_compute/"+nn3)
                    file_size2=str(file_size)+" kb"
                    CREATING=filerecord.objects.create(file=file,filename=nn3,filetype='.xlsx',filename_Timestamp=nn3,RawFileRef_Id=client2['id'],Action_Status='Compute',row_no='NA',sheet_name='NA',HQId='NA',sub_state='NA',stock_id='NA',Branch_id='NA',Merge_by='NA',Merge_Batch_Id='NA',Deleted_by='NA',Archived_by='NA',processed_by='NA',raw_file_path=raw_path,processed_file_path='NA',error_file_path='NA',control_summary_path='NA',Merge_file_path='NA',Company_Id=Company_Id,type=Type,financial_year=year, month=month, file_size=file_size2,Merge_Reference='NA',GSTIN=GSTIN,status=statuss,month2=month2,Row_Size=row_col_size[0]-1,Column_Size=row_col_size[1],Unique_Invoices='na',Total_Tax_Value='na',Total_Taxable_Vale='na')
                    see3 = filerecord.objects.filter(filename=nn3)
                    for i in see3:
                        i.Uploadedtimestamp = time
                        i.Uploaded_by = time
                        i.Timestamp = time
                        i.save()
                    return HttpResponse(json.dumps({"status":"success","reason":"Your File Successfully Processed"}),status=status.HTTP_201_CREATED)
                except KeyError as e:
                    return HttpResponse(json.dumps({"status":"failed","reason": str(e) + " Column is not present in '"+temp_name2+"'template"}),status=status.HTTP_201_CREATED)


class Create_Rule(APIView):
    def post(self,request):
        data_col=request.data['data']
        temp_data=request.data['templateDetails']
        file2=data_col['file']
        fileExtension = data_col['filename'].split(os.extsep)[-1]
        Type=data_col['type']
        print(Type,"type")
        sheetname=temp_data['sheet_name']
        rowno=temp_data['row_no']
        Temp_Name=temp_data['Template_Name']
        print(Temp_Name,"temp name")
        comp=data_col['Company_Id']
        print(comp,"comp name")
        print(file2,"file2222222222")
        if (data_col['Action_Status']=='New' or data_col['Action_Status']=='Merge') and (Type =='GSTR1-Sales'  or Type =='GSTR2-Purchase'):
            d=Filere()
            df_data = d.fileread(fileExtension,sheetname,data_col['filename'],rowno)
            if (isinstance(df_data,str)):
                return HttpResponse(json.dumps({"status": "failed","reason":df_data+" ,Please Create New Template"}), status=status.HTTP_201_CREATED)
            else:
                df2 = df_data.rename(columns=lambda x: x.strip() if (x != None) else x)
                df3 = df2.applymap(lambda x: x.strip() if type(x) == str else x)
                col=list(df3.columns)
                sql_query=list(Mapping.objects.filter(Company_Id=comp,Type=Type,Template_Name=Temp_Name).values())
                print(sql_query,"sql_query")
                sys_col = sql_query[0]['id']
                return HttpResponse(json.dumps({"status":"success","reason":col,"tempid":str(sys_col)}),status=status.HTTP_201_CREATED)

class Mappingview(APIView):
    def post(self,request):
        temp_name=request.data['Template_Name']
        print(temp_name)
        comp_id=request.data['Company_Id']
        print(comp_id)
        type=request.data['type']
        print(type)
        see = list(Mapping.objects.filter(Template_Name=temp_name).values())
        print(see)
        dtaat=see[-1]
        a=dtaat['column_header']
        print(a)
        if a is None:
            return HttpResponse(json.dumps({"status":"failed","reason":"You have not mapped"}))
        else:
            d = json.loads(a)
            print(d,"dddddd")
            ok={"ok":"ok"}
            return HttpResponse(json.dumps(d))

class Delete_temp(APIView):
    def post(self,request):
        process=request.data['process']
        print(process)
        temp_name=request.data['Template_Name']
        print(temp_name)
        comp_id=request.data['Company_Id']
        print(comp_id)
        id_data=request.data['id']
        print(id_data)
        type=request.data['type']
        print(type)
        if process=="Compute" or process=="TG-File":
            deleteing=Mapping.objects.filter(id=id_data).delete()
            return HttpResponse(json.dumps({"status":"success","reason":"Your Template Deleted"}))
        else:
            return HttpResponse(json.dumps({"status":"failed","reason":"Something Went Wrong"}))

class Master_table(APIView):
    def post(self, request):
        data_read = request.data
        print(data_read, "data_read")
        filename = str(data_read['file'])
        sheet = str(data_read['sheet_name'])
        Row = data_read['row_no']
        fileExtension = filename.split(os.extsep)[-1]
        print(filename, sheet, Row, fileExtension)
        print("print")
        if fileExtension == 'xlsb':
            try:
                df_data = []
                xls_file = open_workbook(request.data['file'])
                print("ASFDasdfghjdsdfghjFFSDFSDF")
                with xls_file.get_sheet(sheet) as sheet:
                    for row in sheet.rows():
                        df_data.append([item.v for item in row])
                    df_data = pd.DataFrame(df_data[int(Row):], columns=df_data[int(Row) - 1])
                    print(df_data, "df_data")
                columns = list(df_data.columns)
                print(columns, "col")
                colu = all(isinstance(n, str) for n in columns)
                print(colu, "colucolucolu")
                if (colu == True):
                    master_data = Master_table_serializer(data=request.data)
                    if master_data.is_valid():
                        master_data.save()
                        see3 = Mastertable.objects.filter(id=master_data.data['id']).update(MasterFileColumns=columns)
                        return HttpResponse(json.dumps({"status": "Success", "reason": "Master File Uploaded"}))
                else:
                    df_data = 'Incorrect Row Number'
                return HttpResponse(json.dumps({"status": "failed", "response": df_data}))
            except Exception as e:
                print(e, "EEEEEEEEEEE")
                if str(e).endswith('is not in list'):
                    e = 'Invalid Sheetname'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).endswith('index out of range'):
                    e = 'At Given Row Number Data Not Present'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).startswith('File is not a zip'):
                    e = 'File is corrupted/Unable to read'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).startswith('Passed'):
                    e = 'At Given Row Number Data Not Present'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                else:
                    return HttpResponse(json.dumps({"status": "failed", "response": str(e)}))
        elif (fileExtension == 'xlsx' or fileExtension == 'XLSX' or fileExtension == 'xls'):
            try:
                print("m in xlsx")
                df = pd.read_excel(request.data['file'], sheet_name=str(sheet), header=int(Row) - 1)
                print(df, "xlsx")
                columns = list(df.columns)
                print(columns, "col")
                colu = all(isinstance(n, str) for n in columns)
                print(colu, "colucolucolu")
                if (colu):
                    master_data = Master_table_serializer(data=request.data)
                    if master_data.is_valid():
                        master_data.save()
                        see3 = Mastertable.objects.filter(id=master_data.data['id']).update(MasterFileColumns=columns)
                        return HttpResponse(json.dumps({"status": "Success", "reason": "Master File Uploaded"}))
                else:
                    df_data = 'Incorrect Row Number'
                return HttpResponse(json.dumps({"status": "failed", "response": df_data}))
            except Exception as e:
                print(e)
                if str(e).startswith('No'):
                    e = 'Invalid Sheetname'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).startswith('Unsupported'):
                    e = 'File is corrupted/Unable to read'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                elif str(e).startswith('Passed'):
                    e = 'At Given Row Number Data Not Present'
                    return HttpResponse(json.dumps({"status": "failed", "response": e}))
                else:
                    return HttpResponse(json.dumps({"status": "failed", "response": str(e)}))
        elif fileExtension == 'csv':
            df = pd.read_csv(request.data['file'], encoding="ISO-8859-1")
            print(df, "csv")
            master_data = Master_table_serializer(data=request.data)
            if master_data.is_valid():
                master_data.save()
                see3 = Mastertable.objects.filter(id=master_data.data['id']).update(MasterFileColumns=columns)
                return HttpResponse(json.dumps({"status": "Success", "reason": "Master File Uploaded"}))

class Get_MaaterFile_list(APIView):
    def post(self, request):
        comp_id = request.data['Company_Id']
        type = request.data['Type']
        Company_Id2 = comp_id.replace('"', '')
        dataaa = list(Mastertable.objects.filter(Company_Id=Company_Id2, Type=type).values('id', 'file'))
        print(dataaa, "dataaa")
        file_list = []
        for i in dataaa:
            filename = i['file']
            id = i['id']
            base = os.path.basename(filename)
            file_list.append({"id": id, "file": base})
        print(file_list, "file_list")
        return HttpResponse(json.dumps({"status": "Success", "response": file_list}))

        #

class Masterfile_columns(APIView):
    def post(self, request):
        read_data = request.data
        print(read_data, "read data")
        map1 = list(Mastertable.objects.filter(id=read_data['id']).values('MasterFileColumns'))
        print(map1, "map")
        columns = map1[0]['MasterFileColumns']
        columns2 = ast.literal_eval(columns)
        return HttpResponse(json.dumps({"status": "Success", "response": columns2}), status=status.HTTP_201_CREATED)

class download_file(APIView):
    def post(self,request):
        downoad_data=request.data['data']
        print(downoad_data,"downoad_data")
        filename=downoad_data['filename']
        print(filename,"filename")
        file=downoad_data['raw_file_path']
        print(file,"file")
        folder=downoad_data['Action_Status']
        print(folder,"folder")
        select=request.data['number']
        print(select,"select")
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(BASE_DIR, 'BASE_DIR')

        try:
            if folder=='TG-FILE' and select=='1':
                genCSVPath=(BASE_DIR+"/TG-FILE/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
            elif folder=='TG-FILE' and select=='2':
                genCSVPath=(BASE_DIR+"/Error_files/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
            elif folder=='TG-FILE' and select=='3':
                genCSVPath=(BASE_DIR+"/Control_Summary/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            return e
        try:
            if folder=='New'  and select=='4':
                genCSVPath=(BASE_DIR+"/media/"+file)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                # filen = filename
                response['Content-Disposition'] = b'attachment; filename=%s' % file.encode(encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            print(e,"eeee")
            return HttpResponse(json.dumps({"status": "failed","response":"file is not processed/uploaded on server"}), status=status.HTTP_201_CREATED)
        try:
            if folder=='TG-FILE' and select=='4':
                #print("m in else")
                print("m here for raw")
                print(downoad_data['raw_file_path'],"filefilefile")
                if len(glob.glob(BASE_DIR+"/media/"+downoad_data['raw_file_path'])) != 0:
                    genCSVPath=(BASE_DIR+"/media/"+downoad_data['raw_file_path'])
                else:
                    genCSVPath=(BASE_DIR+"/Merge/"+downoad_data['raw_file_path'])
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                response['Content-Disposition'] = b'attachment; filename=%s' % downoad_data['raw_file_path'].encode(encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                return response

            elif folder=="Compute" and select=='4':
                arr = os.listdir(BASE_DIR+'/Merge/')
                if len(glob.glob(BASE_DIR+"/media/"+file)) != 0:
                    genCSVPath=(BASE_DIR+"/media/"+file)
                else:
                    genCSVPath=(BASE_DIR+"/Merge/"+file)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                response['Content-Disposition'] = b'attachment; filename=%s' % file.encode(encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                return response

        except Exception as e:
            return e
        try:
            if folder=='Merge' and select=='4' :
                genCSVPath=(BASE_DIR+"/Merge/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                response['Content-Disposition'] = b'attachment; filename=%s' % filename.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            return e
        try:
            if folder=='Compute' and select=='1':
                genCSVPath=(BASE_DIR+"/client_compute/"+filename)
                print(genCSVPath,"genCSVPathgenCSVPath")
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                # filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filename.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
            else:
                pass
        except Exception as e:
            return e
#----------------------------------------------Done-----------------------------------------#
#----------------------------------------------Done-----------------------------------------#
        try:
            if folder=='New' and select=='4':
                genCSVPath=(BASE_DIR+"/Merge/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                filen = filename +'.xlsx'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            return e

class testt(APIView):
    def post(self,request):
# rule = pd.read_sql("SELECT * from `tg_comparisionandarithmeticandfuction` WHERE `companyId`='"+str(Company_Id)+"'and Template_Name='"+str(temp_name2)+"'",connection)
        datatatata=list(TgComparisionandarithmeticandfuction2.objects.filter(companyid=2408,template_name='abhil comp').values())
        # (Mastertable.objects.filter(id=read_data['id']).values())
        print(datatatata,"dataaa")
        return HttpResponse(json.dumps({"status": "failed","response":"df_data"}), status=status.HTTP_201_CREATED)


class Sheet_ViewRule(APIView):
    def post(self,request):
        comp=request.data['view_rule_data']
        print(comp)
        map1 = list(Mapping.objects.filter(Company_Id=comp['Company_Id'], Type=comp['type'], Template_Name=comp['Template_Name']).values('row_no','sheet_name'))
        return HttpResponse(json.dumps({"status": "sucesss", "response":map1 }), status=status.HTTP_201_CREATED)

class Download_RuleBuilder_Rule(APIView):
    def post(self,request):
        print("download")
        download = request.data['download_rule_data']
        print("Download rule", download)
        comp=download['Company_Id']
        print(comp)
        check=str(comp.replace('"',''))
        print(check)
        dowwnload_Data = list(TgComparisionandarithmeticandfuction2.objects.filter(companyid=check,template_name=download["Template_Name"]).values())
        return Response({"status": dowwnload_Data},status=status.HTTP_201_CREATED)
