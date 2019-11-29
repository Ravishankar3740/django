from rest_framework.views import APIView
from rest_framework import status
from .serializers import *
import pymysql
import pandas as pd
import json
import os
from itertools import zip_longest
import re
import datetime
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
import datetime
import mysql.connector
from .tg_sales2 import *
from .Dynamic_Rule_Script import *
import xlrd
import glob
from .Sales_validation import *
from .purchase_validation import *
global connection
import time
connection = mysql.connector.connect(host ='52.66.30.64', user="taxgenie", password="taxgenie*#8102*$", database="taxgenie_efilling")


def DBConnection():
    user = 'taxgenie'
    passw = 'taxgenie@123'
    host = '52.66.123.201'
    port = 3306
    database = 'upload'
    mydb = create_engine('mysql+pymysql://' + user + ':' +passw + '@' + host + ':' +str(port) + '/' + database , echo = False)
    return  mydb

def DBConnectionDev():
    user = 'taxgenie'
    passw = 'taxgenie*#8102*$'
    host = '52.66.30.64'
    port = 3306
    database = 'taxgenie_efilling'
    mydb = create_engine('mysql+pymysql://' + user + ':' +passw + '@' + host + ':' +str(port) + '/' + database , echo = False)
    return  mydb


def fileread(fileExtension,sheetname,filename,header):
    print()
    filename =r"/home/ubuntu/RuleBuilder/s3_fileupload/media/"+filename
    if (fileExtension == 'xlsx') | (fileExtension == 'xls')| (fileExtension == 'XLSX'):
        print("m at xlsx")
        df_data = pd.read_excel(filename,sheet_name=sheetname,header=header-1,sort=False)
        print(len(df_data.columns))
    elif fileExtension=='xlsb':
        print("m at xlsb")
        df_data=[]
        xls_file = open_workbook(filename)
        with xls_file.get_sheet(sheetname) as sheet:
            for row in sheet.rows():
                df_data.append([item.v for item in row])
        df_data = pd.DataFrame(df_data[int(header):], columns=df_data[header-1])
        print(len(df_data.columns))
    elif fileExtension=='csv':
        df_data = pd.read_csv(filename,encoding="ISO-8859-1")
    return df_data

class StartView(APIView):
    def post(self,request):
        Company_Id = request.data['Company_Id']
        type=request.data['File_Type']
        month=request.data['month2']
        financial_year=request.data['Financial_Year']
        print(financial_year)
        status2= ['Deleted', 'Uploaded']
        sql_query=list(filerecord.objects.filter(Company_Id=Company_Id,type=type,month2=month,financial_year=financial_year).exclude(Action_Status='Uploaded').exclude(Action_Status='Deleted').order_by('-id').values())
        print(sql_query)
        return Response(({"status": sql_query}),status=status.HTTP_201_CREATED)
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
        print(sql_query,"processsed")
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
            return Response(({"status":sql_query}),status=status.HTTP_201_CREATED)


class UploadView2(APIView):
    parser_classes = (MultiPartParser, FormParser, FileUploadParser)
    def post(self, request):
        upload_data =UploadSerializer(data=request.data)
        #print(upload_data,"data")
        if upload_data.is_valid():
            upload_data.save()
            uppu=upload_data.data
            print(uppu)
            filetype2=os.path.basename(upload_data.data['filetype'])
            file_name1 = os.path.basename(upload_data.data['file'])
            fileExtension = file_name1.split(os.extsep)[-1]
            print(fileExtension,"fileExtension is here")
            #print(file_name1,"start")
            filetype=os.path.basename(upload_data.data['type'])
            #print(filetype)
            filename2 = Path(file_name1).stem
            time = upload_data.data['Timestamp']
            id = upload_data.data['id']
            Company_Id = upload_data.data['Company_Id']
            month = str(upload_data.data['month2'])
            print(month,"here is month2")
            print(type(month))
            Action_Status = upload_data.data['Action_Status']
            if filetype =='GSTR2-Purchase' and fileExtension=='xlsx' or filetype =='GSTR1-Sales' and fileExtension=='xlsx' or filetype =='GSTR2-Purchase' and fileExtension=='xlsb' or filetype =='GSTR1-Sales' and fileExtension=='xlsb':
                datta=time.replace("T","_")
                s3_file_path=filename2+"_"+datta+'.xlsx'
                print(s3_file_path,"s3 name")
                see2 = filerecord.objects.filter(id=id).update(raw_file_path=file_name1)
                # see2 = filerecord.objects.filter(id=id).update(status='New')
                see3 = filerecord.objects.filter(filename_Timestamp='file').update(filename_Timestamp=s3_file_path)
                file_size=os.path.getsize("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file_name1)
                file_size2=str(file_size)+" kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = pd.read_sql("SELECT * from `s3_file_details_filerecord` WHERE `Company_Id`='"+Company_Id+"' and type='"+filetype+"' and month2='"+month+"' ORDER BY id DESC ",connection)
                sql_query['Uploadedtimestamp']=sql_query['Uploadedtimestamp'].astype(str)
                test=sql_query.to_json(orient='records')
                kk= test.replace("\\", '')
                d1= json.loads(kk)
                print(d1)
                return HttpResponse(json.dumps({"status": d1,"status2": "Success","reason":"File Successfully Uploaded"}), status=status.HTTP_201_CREATED)

            elif filetype =='GSTR2-Purchase' and fileExtension=='csv' or filetype =='GSTR1-Sales' and fileExtension=='csv':
                print("m here for csv")
                datta=time.replace("T","_")
                s3_file_path=filename2+"_"+datta+".csv"
                # see2 = filerecord.objects.filter(id=id).update(status='New')
                see2 = filerecord.objects.filter(id=id).update(raw_file_path=file_name1)
                see3 = filerecord.objects.filter(filename_Timestamp='file').update(filename_Timestamp=s3_file_path)
                file_size=os.path.getsize("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file_name1)
                file_size2=str(file_size)+" kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = pd.read_sql("SELECT * from `s3_file_details_filerecord` WHERE `Company_Id`='"+str(Company_Id)+"'and type='"+str(filetype)+"'  ORDER BY id DESC ",connection)
                sql_query['Uploadedtimestamp']=sql_query['Uploadedtimestamp'].astype(str)
                test=sql_query.to_json(orient='records')
                kk= test.replace("\\", '')
                d1= json.loads(kk)
                return HttpResponse(json.dumps({"status": d1,"status2": "Success","reason":"File Successfully Uploaded"}), status=status.HTTP_201_CREATED)
            elif filetype=='General':
                datta=time.replace("T","_")
                # #print(datta)
                s3_file_path=filename2+"_"+datta+".txt"
                see2 = filerecord.objects.filter(id=id).update(raw_file_path=file_name1)
                see3 = filerecord.objects.filter(filename_Timestamp='file').update(filename_Timestamp=s3_file_path)
                file_size=os.path.getsize("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file_name1)
                file_size2=str(file_size)+" kb"
                see3 = filerecord.objects.filter(file_size='new').update(file_size=file_size2)
                sql_query = pd.read_sql("SELECT * from `s3_file_details_filerecord` WHERE `Company_Id`='"+str(Company_Id)+"'and type='"+str(filetype)+"'  ORDER BY id DESC ",connection)
                sql_query['Uploadedtimestamp']=sql_query['Uploadedtimestamp'].astype(str)
                test=sql_query.to_json(orient='records')
                kk= test.replace("\\", '')
                d1= json.loads(kk)
                return HttpResponse(json.dumps({"status": d1,"status2": "Success"}), status=status.HTTP_201_CREATED)
            else:
                if fileExtension!='xlsx' or fileExtension!='csv':
                    deleting=filerecord.objects.filter(id=id).delete()
                    os.remove("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file_name1)
                    return HttpResponse(json.dumps({"status2": "failed","reason":"Only xlsx,csv Files Allowed"}), status=status.HTTP_201_CREATED)


class Delete_files(APIView):
    def post(self, request):
        del_data=request.data['list']
        print(del_data)
        Company_Id=request.data['Company_Id']
        print(Company_Id)
        purpose= request.data['File_Type']
        print(purpose)
        time=str(datetime.datetime.now())
        ls=[]
        for i in del_data:
            ls.append(i.get("id"))
        data_del=tuple(ls)
        print(data_del)
        for i in ls:
            deleting=filerecord.objects.filter(id=i).update(status='Deleted')
            see3 = filerecord.objects.filter(id=i).update(Deleted_at=time)
            see3 = filerecord.objects.filter(id=i).update(Action_Status='Deleted')
            see4 = filerecord.objects.filter(id=i).update(Deleted_by=Company_Id)
        return HttpResponse(json.dumps({"status": "success","response":"Files Deleted"}), status=status.HTTP_201_CREATED)



class hard_query(APIView):
    def post(self,request):
        sheet_row_merge=request.data
        print(sheet_row_merge,"merge data")
        financial_year=sheet_row_merge[0]
        financial_year2=financial_year['financial_year']
        type=sheet_row_merge[0]
        type2=type['type']
        Company_Id=sheet_row_merge[0]
        Company_Id2=Company_Id['companyId']
        statuss=sheet_row_merge[0]
        actionStatus=statuss['actionStatus']
        #print(statuss)
        statuss2=sheet_row_merge[1]
        actionStatus2=statuss['actionStatus']
        #print(statuss2)
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
        cursor = connection.cursor()
        for i in range(0,len(file_id)):
            sql_update_query ="""Update `s3_file_details_filerecord` set sheet_name = %s , row_no = %s  where id= %s """
            #print(sql_update_query)
            input = (sheet[i],header[i],file_id[i])
            cursor.execute(sql_update_query, input)
            connection.commit()
        combined_df=pd.DataFrame()
        for i in file_id:
            print(i,"iiiiiiiiiiiii")
            file = list(filerecord.objects.filter(id=i).values('raw_file_path'))
            print(file,"fileeeeeeeeeeeeeeee")
            filenamee=file[0]
            fileExtension = filenamee['raw_file_path'].split(os.extsep)[-1]
            if fileExtension=='xlsx' or fileExtension=='csv' or fileExtension=='xlsb':
                sql_query = pd.read_sql("SELECT `filename`,`sheet_name`,`row_no` from `s3_file_details_filerecord` WHERE `id`='"+str(i)+"'",connection)
                #print(sql_query)
                file_name=sql_query['filename'].iloc[0]
                fileExtension = file_name.split(os.extsep)[-1]
                file_sheet=sql_query['sheet_name'].iloc[0]
                #print(file_sheet,"sheetnamess")
                file_row=int(sql_query['row_no'].iloc[0])
                try:
                    df_data = fileread(fileExtension,file_sheet,file_name,file_row)
                except Exception as e:
                    return HttpResponse(json.dumps({"status": "failed","response":"Invalid Sheetname"}), status=status.HTTP_201_CREATED)
                # print(df_data)
                try:
                    combined_df=combined_df.append(df_data).reset_index(drop=True)
                except Exception as e:
                    return HttpResponse(json.dumps({"status": "failed","response":"file should be same format"}), status=status.HTTP_201_CREATED)
        print(combined_df,"here is output")
        batch_id=''
        for j in file_id:
            batch_id+=str(j)
        #print(batch_id,"batch")
        #print(len(combined_df))
        time = str(datetime.datetime.now())
        #print(time)
        name='Merge_'+batch_id+'_'+time
        nn=name.replace(':','_')
        nn2=nn.replace('.','_')
        #print(nn2)
        nn3=nn2+'.csv'
        combined_df.to_csv("/home/ubuntu/RuleBuilder/s3_fileupload/Merge/"+nn3,index=False)
        file_size=os.path.getsize("/home/ubuntu/RuleBuilder/s3_fileupload/Merge/"+nn3)
        #print(file_size,"file size path")
        file_size2=str(file_size)+" kb"
        # connection=pymysql.connect(host = '52.66.30.64',user="taxgenie",password="taxgenie*#8102*$",db='taxgenie_efilling')
        cursor = connection.cursor()
        batch_ref='Batch_id_'+batch_id
        sql_insert_query ="INSERT INTO `s3_file_details_filerecord`(`file`,`filename`, `filetype`, `filename_Timestamp`, `return_type`, `Action_Status`, `row_no`, `sheet_name`, `HQId`,`sub_state`, `stock_id`, `Branch_id`, `Merge_by`, `Merge_Batch_Id`, `Deleted_by`,`Archived_by`, `processed_by`, `raw_file_path`, `processed_file_path`,`error_file_path`, `control_summary_path`, `Merge_file_path`,`Company_Id`, `type`, `financial_year`,`month`, `file_size`,`Merge_Reference`,`month2`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        insert_tuple = (file2,nn3,'.xlsx',nn3,'NA','Merge','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA',nn3,'NA','NA','NA','NA',Company_Id2,type2,financial_year2,month2,file_size2,batch_ref,month4)
        cursor.execute(sql_insert_query,insert_tuple)
        connection.commit()
        sql_update_query2 =filerecord.objects.filter(filename_Timestamp=nn3).update(row_no='1')
        sql_update_query2 =filerecord.objects.filter(filename_Timestamp=nn3).update(sheet_name=nn2)
        see3 = filerecord.objects.filter(filename=nn3).update(Uploadedtimestamp=time)
        see3 = filerecord.objects.filter(filename=nn3).update(Uploaded_by=time)
        see3 = filerecord.objects.filter(filename=nn3).update(Timestamp=time)
        return HttpResponse(json.dumps({"status": "success","response":"Your Files Successfully Mergerd"}), status=status.HTTP_201_CREATED)


class Mapping_file_sheets(APIView):
    def post(self, request):
        Company_Id = request.data['Company_Id']
        #print(Company_Id)
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
        print("111")
        if actionStatus=='New' and (type=='GSTR1-Sales' or type=='GSTR2-Purchase') or actionStatus=='Compute' and (type=='GSTR1-Sales' or type=='GSTR2-Purchase') or actionStatus=='Merge' and (type=='GSTR1-Sales' or type=='GSTR2-Purchase'):
            if fileExtension =='xlsx':
                xl = pd.ExcelFile('/home/ubuntu/RuleBuilder/s3_fileupload/media/'+filename)
                sheetnames=xl.sheet_names
                return HttpResponse(json.dumps({"sheet_names": sheetnames}), status=status.HTTP_201_CREATED)
            elif fileExtension =='xlsb':
                wb = open_workbook('/home/ubuntu/RuleBuilder/s3_fileupload/media/'+filename)
                sheetnames=wb.sheets
                return HttpResponse(json.dumps({"sheet_names": sheetnames}), status=status.HTTP_201_CREATED)
            elif fileExtension =='csv':
                print("222")
                return HttpResponse(json.dumps({"sheet_names": file}), status=status.HTTP_201_CREATED)
        else:
            return HttpResponse(json.dumps({"sheet_names": "file"}), status=status.HTTP_201_CREATED)

class file_details_sheet_row(APIView):
    def post(self, request):
        data3=request.data
        print(data3)
        Template_Name=data3['Template_Name']
        comp_id=data3['Company_Id']
        Type=data3['Type']
        queryset_1 =list(Mapping.objects.filter(Company_Id=comp_id,Type=Type).values('Template_Name'))
        for i in queryset_1:
            if i['Template_Name']==Template_Name:
                return HttpResponse(json.dumps({"status2":"exists","status": "Template Name already exists"}))
        sheet_row_data = Mapping_serializer(data=request.data)
        if sheet_row_data.is_valid():
            sheet_row_data.save()
            file_sheet=sheet_row_data.data['sheet_name']
            file_row=int(sheet_row_data.data['row_no'])-1
            Action_Status=sheet_row_data.data['Action_Status']
            file_filename=sheet_row_data.data['filename']
            fileExtension = file_filename.split(os.extsep)[-1]
            if Action_Status =='New':
                if fileExtension=="xlsx":
                    df=pd.read_excel('/home/ubuntu/RuleBuilder/s3_fileupload/media/'+file_filename,sheet_name=file_sheet,header=file_row)
                    columns=list(df.columns)
                    # print(columns,"columns")
                    colu=all(isinstance(n, str) for n in columns)
                    # print(colu,"colu")
                    if colu==True:
                        print("m in if")
                        return HttpResponse(json.dumps({"status":columns,"final_status":"success"}), status=status.HTTP_201_CREATED)
                    elif colu==False:
                        return HttpResponse(json.dumps({"reason":"Incorrect Row Number Of File","final_status":"failed"}), status=status.HTTP_201_CREATED)

                elif fileExtension=="xlsb":
                    df_data=[]
                    xls_file = open_workbook('/home/ubuntu/RuleBuilder/s3_fileupload/media/'+file_filename)
                    with xls_file.get_sheet(file_sheet) as sheet:
                        for row in sheet.rows():
                            df_data.append([item.v for item in row])
                    df_data = pd.DataFrame(df_data[int(file_row+1):], columns=df_data[file_row])
                    print(df_data)
                    columns=list(df_data.columns)
                    colu=all(isinstance(n, str) for n in columns)
                    print(colu,"colu")
                    if colu==True:
                        print("m in if")
                        return HttpResponse(json.dumps({"status":columns,"final_status":"success"}), status=status.HTTP_201_CREATED)
                    elif colu==False:
                        return HttpResponse(json.dumps({"reason":"Incorrect Row Number Of File","final_status":"failed"}), status=status.HTTP_201_CREATED)
                elif fileExtension=="csv":
                    df=pd.read_csv('/home/ubuntu/RuleBuilder/s3_fileupload/media/'+file_filename,encoding="ISO-8859-1")
                    columns=list(df.columns)
                    colu=all(isinstance(n, str) for n in columns)
                    print(colu,"colu")
                    if colu==True:
                        print("m in if")
                        return HttpResponse(json.dumps({"status":columns,"final_status":"success"}), status=status.HTTP_201_CREATED)
                    elif colu==False:
                        return HttpResponse(json.dumps({"reason":"Incorrect Row Number Of File","final_status":"failed"}), status=status.HTTP_201_CREATED)
            elif Action_Status =='Compute' and fileExtension=="csv":
                df=pd.read_csv('/home/ubuntu/RuleBuilder/s3_fileupload/client_compute/'+file_filename)
                columns=list(df.columns)
                colu=all(isinstance(n, str) for n in columns)
                print(colu,"colu")
                if colu==True:
                    print("m in if")
                    return HttpResponse(json.dumps({"status":columns,"final_status":"success"}), status=status.HTTP_201_CREATED)
                elif colu==False:
                    return HttpResponse(json.dumps({"reason":"Incorrect Row Number Of File","final_status":"failed"}), status=status.HTTP_201_CREATED)
            elif Action_Status =='Merge' and fileExtension=="csv":
                df=pd.read_csv('/home/ubuntu/RuleBuilder/s3_fileupload/Merge/'+file_filename,encoding="ISO-8859-1")
                columns=list(df.columns)
                colu=all(isinstance(n, str) for n in columns)
                print(colu,"colu")
                if colu==True:
                    print("m in if")
                    return HttpResponse(json.dumps({"status":columns,"final_status":"success"}), status=status.HTTP_201_CREATED)
                elif colu==False:
                    return HttpResponse(json.dumps({"reason":"Incorrect Row Number Of File","final_status":"failed"}), status=status.HTTP_201_CREATED)




class system_col(APIView):
    def post(self, request):
        base = request.data['invoiceType']
        type1 = request.data['type']
        if len(base) == 1:
            invoice_type = 'where invoiceType = "'+base[0]+'" and type = "'+type1+'"'
        else:
            invoice_type = 'where invoiceType in '+str(tuple(base))+' and type = "'+type1+'"'
        sysCol = pd.read_sql("SELECT DISTINCT `field`, `required` FROM `system col_s3` "+invoice_type, connection)
        sys_col = sysCol.to_json(orient='records')
        return HttpResponse(json.dumps(sys_col),status=status.HTTP_201_CREATED)

class Col_header(APIView):
    def post(self, request):
        temp_name = request.data['Template_Name']
        #print(temp_name,"its temp_name")
        column_header = request.data['column_header']
        #print(column_header)
        Company_Id = request.data['Company_Id']
        #print(Company_Id)
        see3 = Mapping.objects.filter(Template_Name=temp_name).update(column_header=column_header)
        return HttpResponse(json.dumps({"status": "success"}), status=status.HTTP_201_CREATED)


class choose_template(APIView):
    def post(self, request):
        selection=request.data['number']
        print(selection)
        Company_Id = request.data['Company_Id']
        print(Company_Id)
        Action_Status = request.data['Action_Status']
        print(Action_Status)
        type = request.data['Type']
        print(type)
        if selection=='2' or selection=='1' :#tg
            connection = mysql.connector.connect(host = '52.66.30.64', user="taxgenie", password="taxgenie*#8102*$", database="taxgenie_efilling")
            sql_query = pd.read_sql("SELECT `id` ,`Template_Name` from `s3_file_details_mapping` WHERE `Company_Id`='"+Company_Id+"' and type='"+str(type)+"' and  Action_Status='"+str(Action_Status)+"' ORDER BY id DESC ",connection)
            print(sql_query)#
            sys_col = sql_query['Template_Name'].values.tolist()
            sys_id = sql_query['id'].values.tolist()
            num=[]
            reason1 = []
            for i in range(0,len(sys_id)):
                num.append("temp_id")
                r = dict(zip(num, sys_id))
                reason1.append(r)
            #print(reason1,"reason 1")
            col=[]
            reason2 = []
            for i in range(0,len(sys_col)):
                col.append("temp_name")
                r = dict(zip(col, sys_col))
                reason2.append(r)
            #print(reason2,"reason 2")
            list1=[]
            sys_col2=[{**u, **v} for u, v in zip_longest(reason1, reason2, fillvalue={})]
            #print(sys_col2,"here is final data")
            return HttpResponse(json.dumps({"status":"success","reason":sys_col2}),status=status.HTTP_201_CREATED)
        else:
            return HttpResponse(json.dumps({"success":"unsuccess"}),status=status.HTTP_201_CREATED)


class Tg_File_Processing(APIView):
    def post(self, request):
        try:
            Action_Status=request.data['actionstatus']
            temp_name = request.data['Template_Name']
            temp_name2=temp_name['temp_name']
            raw_path = request.data['raw_file_path']
            month2 = request.data['month2']
            month = request.data['month']
            year = request.data['financial_year']
            tg = request.data['Tg_Data']
            print(tg,"tggggggggggg")
            Type = request.data['Type']
            Company_Id = request.data['Company_Id']
            file = request.data['filename']
            print(file,"processing file")
            fileExtension = file.split(os.extsep)[-1]
            print(fileExtension,"fileExtension is here")
            gstin="27AAECS1548J1Z6"
            if Action_Status=='New' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase') or Action_Status=='Compute' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase')  or  Action_Status=='Merge' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase'):
                sql_query = pd.read_sql("SELECT * from `s3_file_details_mapping` WHERE `Company_Id`='"+Company_Id+"'and type='"+str(Type)+"' and Template_Name='"+str(temp_name2)+"'",connection)
                print(sql_query)
                row=sql_query['row_no'].iloc[0]
                sheet=sql_query['sheet_name'].iloc[0]
                map1= pd.read_sql("select `id`,`column_header` from  `s3_file_details_mapping` WHERE `Company_Id`='"+Company_Id+"'and type='"+str(Type)+"' and Template_Name=Binary'"+str(temp_name2)+"'",connection)
                print(map1,"mapping")
                temp_id=int(map1['id'].iloc[0])
                bb = map1.to_json(orient='records')
                kk = bb.replace("\\", '')
                kk1 = kk[1:-1]
                data = re.search(r'(?=\[)(.*\])', str(kk1)).group(1)
                dt = {}
                data1 = eval(data)
                for d in data1:
                    dt.update(d)
                if len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file)) != 0:
                    path='/home/ubuntu/RuleBuilder/s3_fileupload/media/'+file
                    print("m in media")
                    statuss='TG-FILE'
                else:
                    print("m in compute")
                    path='/home/ubuntu/RuleBuilder/s3_fileupload/client_compute/'+file
                    statuss='Compute'
                print(path)
                if fileExtension=='xlsx':
                    df2=pd.read_excel(path,sheet_name=sheet,header=int(row)-1)
                elif fileExtension=='xlsb':
                    df2=[]
                    print(row)
                    print(type(row))
                    row2=int(row)
                    print(row)
                    print(type(row))
                    xls_file = open_workbook(path)
                    with xls_file.get_sheet(sheet) as sheet:
                        for row in sheet.rows():
                            df2.append([item.v for item in row])
                    df2 = pd.DataFrame(df2[row2:],columns=df2[row2-1])
                elif fileExtension=='csv':
                    df2=pd.read_csv(path,encoding="ISO-8859-1")
                df3 = df2.rename(columns=lambda x: x.strip() if (x != None) else x)
                df3 = df2.applymap(lambda x: x.strip() if type(x) == str else x)
                df3['M_id']=df3.index+1
                stage_tb = pd.DataFrame()
                for user,sys in dt.items():
                    # print(df2[user])
                    # print(sys,"sys")
                    stage_tb[sys]=df3[user]
                stage_tb['M_id']=df3['M_id']
                stage_tb['M_id']
                print(stage_tb.columns,"stage_tb")
                if Action_Status=='New' or Action_Status=='Compute'  and Type=='GSTR1-Sales':
                    tg=Tg_Processing()
                    print("for process")
                    output=tg.Sales(stage_tb)
                    print(output.columns,"for upload")
                elif Action_Status=='New'or Action_Status=='Compute' and Type=='GSTR2-Purchase':
                    tg=Tg_Processing()
                    output=tg.Purchase(stage_tb)

                output[['Taxable Value','Invoice Value','Igst Amount','Sgst Amount','Cgst Amount']]=output[['Taxable Value','Invoice Value','Igst Amount','Sgst Amount','Cgst Amount']].replace("na",0).astype(str).convert_objects(convert_numeric=True).fillna(0).astype(float)
                print(output.columns,"output")
                output1=pd.pivot_table(output, index=['Buyer Gstin','Status'],values=['Taxable Value','Invoice Value','Igst Amount','Sgst Amount','Cgst Amount'], aggfunc={'Status':'count','Taxable Value':'sum','Invoice Value':'sum','Igst Amount':'sum','Cgst Amount':'sum','Sgst Amount':'sum'})
                print(output1.columns,"output1")
                print("here")
                output=pd.merge(left=df3, right=output[['M_id','Status','reason']], on=['M_id'], how='left')
                print("there")
                del output['M_id']
                time = str(datetime.datetime.now())
                filename_wout_ext= Path(raw_path).stem
                name=str(filename_wout_ext)+'_'+time
                nn=name.replace(':','_')
                nn2=nn.replace('.','_')
                nn3=nn2+'.csv'
                success = output.loc[output['reason']=='']
                success.to_csv("/home/ubuntu/RuleBuilder/s3_fileupload/TG-FILE/"+nn3,index=False)
                Fail = output.loc[output['reason']!='']
                Fail.to_csv("/home/ubuntu/RuleBuilder/s3_fileupload/Error_files/"+nn3,index=False)
                output1.to_csv("/home/ubuntu/RuleBuilder/s3_fileupload/Control_Summary/"+nn3,index=False)
                file_size=os.path.getsize("/home/ubuntu/RuleBuilder/s3_fileupload/TG-FILE/"+nn3)
                file_size2=str(file_size)+" kb"
                cursor = connection.cursor()
                sql_insert_query ="INSERT INTO `s3_file_details_filerecord`(`file`,`filename`, `filetype`, `filename_Timestamp`, `return_type`, `Action_Status`, `row_no`, `sheet_name`, `HQId`,`sub_state`, `stock_id`, `Branch_id`, `Merge_by`, `Merge_Batch_Id`, `Deleted_by`,`Archived_by`, `processed_by`, `raw_file_path`, `processed_file_path`,`error_file_path`, `control_summary_path`, `Merge_file_path`,`Company_Id`, `type`, `financial_year`,`month`, `file_size`,`Merge_Reference`,GSTIN,template_id,status,month2) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                insert_tuple = (nn3,nn3,'.csv',nn3,'NA','TG-FILE','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA',raw_path,'NA','NA','NA','NA',Company_Id,Type,year, month, file_size2,'-',gstin,temp_id,statuss,month2)
                cursor.execute(sql_insert_query,insert_tuple)
                connection.commit()
                see3 = filerecord.objects.filter(filename=nn3).update(Uploadedtimestamp=time)
                see3 = filerecord.objects.filter(filename=nn3).update(Uploaded_by=time)
                see3 = filerecord.objects.filter(filename=nn3).update(Timestamp=time)
                return HttpResponse(json.dumps({"status":"success","reason":"Your File Successfully Processed"}),status=status.HTTP_201_CREATED)
        except KeyError as e:
            return HttpResponse(json.dumps({"status":"failed","reason":f"{e} Columns  not present in File columns"}),status=status.HTTP_201_CREATED)

class client_file_processing(APIView):
    def post(self, request):
        # print("in client")
        temp_name = request.data['Template_Name']
        #print(temp_name)
        client = request.data['Client_Data']
        # print(client,"clienttttttttt")
        client2=client[0]
        temp_name2=temp_name['temp_name']
        raw_path = request.data['raw_file_path']
        # print("asdsdkasbdf")
        year = request.data['financial_year']
        month = request.data['month']
        month2 = request.data['month2']
        # GSTIN = request.data['GSTIN']
        GSTIN="27AAECS1548J1Z6"
        Type = request.data['Type']
        Company_Id = request.data['Company_Id']
        file=request.data['file']
        actionstatus=request.data['actionstatus']
        # print(file,"filename")
        fileExtension = client2['filename'].split(os.extsep)[-1]
        # print(fileExtension,"fileExtension is here")
        if actionstatus=='New' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase') or actionstatus=='Merge' and (Type=='GSTR1-Sales' or Type=='GSTR2-Purchase'):
            sql_query = pd.read_sql("SELECT * from `s3_file_details_mapping` WHERE `Company_Id`='"+Company_Id+"'and type='"+str(Type)+"' and Template_Name='"+str(temp_name2)+"'",connection)
            # print(sql_query,"heeheheheheheh")
            row=sql_query['row_no'].iloc[0]
            # print(row)
            sheet=sql_query['sheet_name'].iloc[0]
            # print(sheet)
            if len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+client2['filename'])) != 0:
                path='/home/ubuntu/RuleBuilder/s3_fileupload/media/'+client2['filename']
                print("m in media")
                statuss='Compute'
            else:
                print("m in compute")
                path='/home/ubuntu/RuleBuilder/s3_fileupload/Merge/'+client2['filename']
                statuss='Compute'
            if fileExtension=='xlsx':
                df2=pd.read_excel(path,sheet_name=sheet,header=int(row)-1)
                df2 = df2.rename(columns=lambda x: x.strip())
            elif fileExtension=='xlsb':
                df2=[]
                print(row)
                print(type(row))
                row2=int(row)
                print(row)
                print(type(row))
                # print(row-1,"minus")
                xls_file = open_workbook(path)
                with xls_file.get_sheet(sheet) as sheet:
                    for row in sheet.rows():
                        df2.append([item.v for item in row])
                df2 = pd.DataFrame(df2[row2:],columns=df2[row2-1])
                df2 = df2.rename(columns=lambda x: x.strip())
            elif fileExtension=='csv':
                # print("csv")
                df2=pd.read_csv(path,encoding="ISO-8859-1")
            rule = pd.read_sql("SELECT * from `tg_comparisionandarithmeticandfuction` WHERE `companyId`='"+str(Company_Id)+"'and Template_Name='"+str(temp_name2)+"'",connection)
            output=purchase_engine(df2,rule)
            time = str(datetime.datetime.now())
            filename_wout_ext= Path(file).stem
            name=filename_wout_ext+'_'+time
            nn=name.replace(':','_')
            nn2=nn.replace('.','_')
            time = str(datetime.datetime.now())
            nn3=nn2+'.csv'
            output.to_csv("/home/ubuntu/RuleBuilder/s3_fileupload/client_compute/"+nn3)
            file_size=os.path.getsize("/home/ubuntu/RuleBuilder/s3_fileupload/client_compute/"+nn3)
            file_size2=str(file_size)+" kb"
            cursor = connection.cursor()
            sql_insert_query ="INSERT INTO `s3_file_details_filerecord`(`file`,`filename`, `filetype`, `filename_Timestamp`, `return_type`, `Action_Status`, `row_no`, `sheet_name`, `HQId`,`sub_state`, `stock_id`, `Branch_id`, `Merge_by`, `Merge_Batch_Id`, `Deleted_by`,`Archived_by`, `processed_by`, `raw_file_path`, `processed_file_path`,`error_file_path`, `control_summary_path`, `Merge_file_path`,`Company_Id`, `type`, `financial_year`,`month`, `file_size`,`Merge_Reference`,GSTIN,status,month2) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            insert_tuple = (file,nn3,'.csv',nn3,'NA','Compute','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA',raw_path,'NA','NA','NA','NA',Company_Id,Type,year, month, file_size2,'-',GSTIN,statuss,month2)
            cursor.execute(sql_insert_query,insert_tuple)
            connection.commit()
            see3 = filerecord.objects.filter(filename=nn3).update(Uploadedtimestamp=time)
            see3 = filerecord.objects.filter(filename=nn3).update(Uploaded_by=time)
            see3 = filerecord.objects.filter(filename=nn3).update(Timestamp=time)
            return HttpResponse(json.dumps({"status":"success","reason":"Your File Successfully Processed"}),status=status.HTTP_201_CREATED)


class download_file(APIView):
    def post(self,request):
        # try:
        downoad_data=request.data['data']
        #print(downoad_data)
        filename=downoad_data['filename']
        #print(filename, "  File name")
        file=downoad_data['raw_file_path']
        #print(file,"rawww")
        folder=downoad_data['Action_Status']
        #print(folder,"folder.............")
        select=request.data['number']
        #print(select)
        try:
            if folder=='TG-FILE' and select=='1':
                #print("m in tgfile and one")
                genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/TG-FILE/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
            elif folder=='TG-FILE' and select=='2':
                genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/Error_files/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
            elif folder=='TG-FILE' and select=='3':
                genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/Control_Summary/"+filename)
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
                #print("m in try")
                genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                # filen = filename
                response['Content-Disposition'] = b'attachment; filename=%s' % file.encode(encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            return e
        try:
            if folder=='TG-FILE' and select=='4':
                #print("m in else")

                arr = os.listdir('/home/ubuntu/RuleBuilder/s3_fileupload/Merge/')
                if len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file)) != 0:
                    genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file)
                else:
                    genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/Merge/"+file)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                filen = filename +'.xlsx'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                response['File_Type'] = 'xlsx'
                return response

            elif folder=="Compute" and select=='4':
                #print("m in else")

                arr = os.listdir('/home/ubuntu/RuleBuilder/s3_fileupload/Merge/')
                #print(arr, " file directory list")
                #print(len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file)), " file directory check 2")

                if len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file)) != 0:
                    genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+file)
                else:
                    genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/Merge/"+file)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                filen = filename +'.xlsx'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="utf-8")
                response['Cache-Control'] = 'no-cache'
                response['File_Type'] = 'xlsx'
                return response

        except Exception as e:
            return e
        try:
            if folder=='Merge' and select=='4' :
                genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/Merge/"+file)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response

        except Exception as e:
            return e



        try:
            if folder=='Compute' and select=='1':
                genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/client_compute/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/octet-stream")
                filen = filename + '.csv'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
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
                genCSVPath=("/home/ubuntu/RuleBuilder/s3_fileupload/Merge/"+filename)
                FilePointer = open(genCSVPath, "rb")
                response = HttpResponse(FilePointer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                filen = filename +'.xlsx'
                response['Content-Disposition'] = b'attachment; filename=%s' % filen.encode(encoding="ISO-8859-1")
                response['Cache-Control'] = 'no-cache'
                return response
        except Exception as e:
            return e




class Create_Rule(APIView):
    def post(self,request):
        data_col=request.data['data']
        temp_data=request.data['templateDetails']
        file2=data_col['file']
        fileExtension = data_col['filename'].split(os.extsep)[-1]
        Type=data_col['type']
        sheetname=temp_data['sheet_name']
        rowno=temp_data['row_no']
        Template_Name=temp_data['Template_Name']
        Company_Id=data_col['Company_Id']
        print(file2,"file2222222222")
        if len(glob.glob("/home/ubuntu/RuleBuilder/s3_fileupload/media/"+data_col['filename'])) != 0:
            path="/home/ubuntu/RuleBuilder/s3_fileupload/media/"+data_col['filename']
            print("m in media")
        else:
            print("m in merge")
            path='/home/ubuntu/RuleBuilder/s3_fileupload/Merge/'+data_col['filename']
        if (data_col['Action_Status']=='New' or data_col['Action_Status']=='Merge') and (Type =='GSTR1-Sales'  or Type =='GSTR2-Purchase'):
            if fileExtension=='xlsx':
                df2=pd.read_excel(path,sheet_name=sheetname,header=int(rowno)-1)
            elif fileExtension=='xlsb':
                df2=[]
                print(rowno)
                print(type(rowno))
                row2=int(rowno)
                print(rowno)
                print(type(rowno))
                xls_file = open_workbook(path)
                with xls_file.get_sheet(sheetname) as sheet:
                    for row in sheet.rows():
                        df2.append([item.v for item in row])
                df2 = pd.DataFrame(df2[row2:],columns=df2[row2-1])
                df2 = df2.rename(columns=lambda x: x.strip())
            elif fileExtension=='csv':
                print("m in merge")
                df2=pd.read_csv(path, encoding="ISO-8859-1")
            df2 = df2.rename(columns=lambda x: x.strip() if (x != None) else x)
            df2 = df2.applymap(lambda x: x.strip() if type(x) == str else x)
            col=list(df2.columns)
            # connection = mysql.connector.connect(host ='52.66.30.64', user="taxgenie", password="taxgenie*#8102*$", database="taxgenie_efilling")
            sql_query = pd.read_sql("SELECT `id` from `s3_file_details_mapping` WHERE `Company_Id`='"+str(Company_Id)+"' and type='"+str(Type)+"' and Template_Name='"+str(Template_Name)+"'",connection)
            sys_col = sql_query['id'].tolist()
            print(sys_col,"sys_colsys_colsys_col")
            colu=all(isinstance(n, str) for n in col)
            print(colu,"colu")
            if colu==True:
                print("m in if")
                return HttpResponse(json.dumps({"status":"success","reason":col,"tempid":str(sys_col[0])}),status=status.HTTP_201_CREATED)
            elif colu==False:
                return HttpResponse(json.dumps({"reason":"Incorrect Row Number Of File","final_status":"failed"}), status=status.HTTP_201_CREATED)


class upload_engine(APIView):
    def post(self,request):
        mydb=DBConnectionDev() # DATABASE CONNECTION
        data_up_eng=request.data['data']
        print(data_up_eng,"uploadengine")
        Action_Status=data_up_eng['Action_Status']
        type=data_up_eng['type']
        pan_num = data_up_eng['GSTIN'][2:-3]
        print(data_up_eng['file'],"filenameee")
        if Action_Status=='TG-FILE' and type=='GSTR1-Sales':
            df_raw=pd.read_csv('/home/ubuntu/RuleBuilder/s3_fileupload/TG-FILE/'+data_up_eng['file'],encoding="ISO-8859-1")
            df_raw[df_raw.columns.dropna()]
            connection=pymysql.connect(host = '52.66.30.64',user = 'taxgenie',password="taxgenie*#8102*$",db='taxgenie_efilling')
            map1= pd.read_sql("select `Template_Name`,`column_header` from  `s3_file_details_mapping` WHERE `Company_Id`='"+data_up_eng['Company_Id']+"'and type='"+data_up_eng['type']+"' and id='"+data_up_eng['template_id']+"'",connection)
            print(map1,"mapping")
            temp_name=str(map1['Template_Name'].iloc[0])
            bb = map1.to_json(orient='records')
            kk = bb.replace("\\", '')
            kk1 = kk[1:-1]
            data = re.search(r'(?=\[)(.*\])', str(kk1)).group(1)
            dt = {}
            data1 = eval(data)
            for d in data1:
                dt.update(d)
            print(dt)
            stage_tb = pd.DataFrame()
            for user,sys in dt.items():
                stage_tb[sys]=df_raw[user]
            stage_tb['id']=df_raw['id']

            time_cal=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_col=datetime.datetime.now().strftime("REFR-"+data_up_eng['GSTIN']+time_cal+data_up_eng['type'])
            df_raw.insert(loc=5, column='Reference_id', value=new_col)
            df_raw.insert(loc=6,column='id',value=df_raw.index)
            print(df_raw.columns, 'raw df updated')

            ls = stage_tb["Seller Gstin"].unique().tolist()
            df_final_s= pd.DataFrame()

            for j in ls:
                df_f=stage_tb.loc[stage_tb['Seller Gstin']==j]
                sellerID = pd.read_sql("select companyID from company_master where GSTNINNO  ='" + j + "'", mydb)
                df_f['sellerID'] = sellerID['companyID'].loc[0]
                new_Sel = datetime.datetime.now().strftime("REFR-" +j+ time_cal + data_up_eng['type'])
                df_f['Reference_id']=new_Sel
                df_f = df_f.reset_index()
                df = Common_check(df_f)
                fail = df.loc[df['reason'] != '']
                Pass = df.loc[df['reason'] == '']
                ls12 = list(Pass['Invoice Type'].unique())
                df3 = pd.DataFrame()

                for i in ls12:
                    if (i.upper() == 'B2B'):
                        b2b = Pass.loc[(Pass['Invoice Type'] == 'B2B')]
                        #         print(len(b2b))
                        df2 = B2B_validation(b2b)
                    elif (i.upper() == 'B2CS'):
                        b2c = Pass.loc[(Pass['Invoice Type'].str.upper() == 'B2CS')]
                        #         print(len(b2c))
                        df2 = B2C_validation(b2c)
                    elif (i.upper() == 'B2CL'):
                        b2c = Pass.loc[(Pass['Invoice Type'].str.upper() == 'B2CL')]
                        #         print(len(b2c))
                        df2 = B2C_validation(b2c)
                    elif (i.upper() == 'CNUR'):
                        cdnur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'CNUR')]
                        #         print(len(cdnur))
                        df2 = CDNUR_validation(cdnur)
                    elif (i.upper() == 'DNUR'):
                        cdnur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'DNUR')]
                        #         print(len(cdnur))
                        df2 = CDNUR_validation(cdnur)
                    elif (i.upper() == 'CNR'):
                        cdnr = Pass.loc[(Pass['Invoice Type'].str.upper() == 'CNR')]
                        #         print(len(cdnr))
                        df2 = CDNR_validation(cdnr)


                    elif (i.upper() == 'DNR'):

                        cdnr = Pass.loc[(Pass['Invoice Type'].str.upper() == 'DNR')]
                        #         print(len(cdnr))
                        df2 = CDNR_validation(cdnr)


                    elif (i.upper() == 'EXP'):

                        exp = Pass.loc[(Pass['Invoice Type'].str.upper() == 'EXP')]
                        #         print(len(exp))
                        df2 = Export_validation(exp)

                    df3 = df3.append(df2, sort=True)



                df_final=fail.append(df3, sort=True)
                df_final_s = pd.DataFrame()
                df_final_s=df_final_s.append(df_final, sort=True)




            if 'index' in df_final_s.columns.tolist():
                del df_final_s['index']
            if 'level_0' in df_final_s.columns.tolist():
                del df_final_s['level_0']
            if 'Reason' in df_final_s.columns.tolist():
                del df_final_s['Reason']

            df_final_s['Status'] = np.where((df_final_s['reason']==''), 'Success', 'Fail')
            df_final_s['invoiceFinancialPeriod'] = data_up_eng['financial_year']
            df_final_s['financialPeriod'] = '052019'
            df_final_s['gstnStatus'] = 'notuploaded'
            df_final_s['invoiceStatus'] = 'Y'
            df_final_s = df_final_s.fillna('')

            #++++++++++++++++++++++++++++++++++++++++++++Result table sales+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


            df_final_s.astype(str).to_sql(name='upload_sales_result_table', con=mydb, if_exists='append', index=False,chunksize=500)
            print(df_final_s['Status'],"data dumped into result table")

    # ######################################################################   DATABASE DUMPING START  ###################################################################################################

            print("##################################################### DB DUMPING START #################################################################################")
            mydb = DBConnectionDev()
            # df11 = pd.read_sql("SELECT * FROM upload_sales_result_table where Reference_id like '%%"+pan_num+"%%"+time_cal+data_up_eng['type']"' and Status='Success'" , mydb)
            df11 = pd.read_sql("SELECT * FROM upload_sales_result_table where Reference_id like '%%"+pan_num+"%%"+time_cal+data_up_eng['type']+"' and Status='Success' ",mydb)
            print(len(df11),"Success data from result sales table")



            if(len(df11) != 0):
                df11.rename(columns={
                    'Buyer Gstin': 'buyerGSTIN',
                    'Invoice Type': 'typeofinvoice',
                    'Invoice SubType': 'invoiceSubType',
                    'Invoice Date': 'invoiceDate',
                    'Invoice No': 'invoiceNo',
                    'Invoice Value': 'invoiceValue',
                    'quantity': 'quantity',
                    'Reverse Charge': 'reverseCharge',
                    'Seller Gstin': 'sellerGSTIN',
                    'unit': 'unit',
                    'Place Of Supply': 'pos',
                    'Taxable Value': 'taxableValue',
                    'Igst Rate': 'igstRate',
                    'Cgst Rate': 'cgstRate',
                    'Sgst Rate': 'sgstOrUgstRate',
                    'Igst Amount': 'igstAmt',
                    'Cgst Amount': 'cgstAmt',
                    'Sgst Amount': 'sgstOrUgstAmt',
                    'Cess Amount': 'cessAmt',
                    'Hsn Code': 'HSNorSAC',
                    'Rate': 'gstRate',
                    'invoiceFinancialPeriod': 'invoiceFinancialPeriod',
                    'Reference_id': 'Reference_id'}, inplace=True)




                header_ls = ['buyerGSTIN', 'typeofinvoice', 'invoiceSubType', 'invoiceDate', 'invoiceNo',
                             'invoiceValue', 'sellerGSTIN', 'pos', 'reverseCharge', 'Reference_id',
                             'invoiceFinancialPeriod','sellerID','financialPeriod','gstnStatus','invoiceStatus']
                df11['buyerGSTIN'] = df11['buyerGSTIN'].astype(str).str.replace('nan', '')

                d = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + data_up_eng['financial_year'] + "'", mydb)
                # d = d.applymap(lambda x: x.strip() if type(x) == str else x)
                d = d.fillna('')
                df11 = df11.fillna('')



                df1 = df11[header_ls].drop_duplicates(subset=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'], keep='first')
                replace = df1.loc[((df1['sellerGSTIN'].isin(d['sellerGSTIN'])) & (df1['invoiceNo'].isin(d['invoiceNo'])) & (df1['typeofinvoice'].isin(d['typeofinvoice'])) & (df1['buyerGSTIN'].isin(d['buyerGSTIN'])) & (df1['invoiceFinancialPeriod'].isin(d['invoiceFinancialPeriod'])))]
                print(len(replace),'Number of data to be replaced')


                insert = df1[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'invoiceSubType', 'gstnStatus', 'sellerID','invoiceStatus', 'financialPeriod', 'buyerGSTIN', 'invoiceDate', 'reverseCharge', 'pos','invoiceFinancialPeriod']].loc[~((df1['sellerGSTIN'].isin(d['sellerGSTIN'])) & (df1['invoiceNo'].isin(d['invoiceNo'])) & (df1['typeofinvoice'].isin(d['typeofinvoice'])) & (df1['buyerGSTIN'].isin(d['buyerGSTIN'])))]
                print(len(replace),'Number of data to insert')
                insert.to_sql('sales_invoice_header', mydb, if_exists='append', index=False, chunksize=100)



                if (len(replace) != 0):
                    print(replace,"I am in replace")

                    count = 0
                    for index, replace in replace.iterrows():
                        count = count + 1
                        updatedAt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        query = """update sales_invoice_header a
                        set
                        a.invoiceSubType='""" + str(replace['invoiceSubType']) + """',
                        a.invoiceDate = '""" + str(replace['invoiceDate']) + """',
                        a.reverseCharge = '""" + str(replace['reverseCharge']) + """',
                        a.invoiceFinancialPeriod ='""" + str(replace['invoiceFinancialPeriod']) + """',
                        a.pos= '""" + str(replace['pos']) + """',
                        a.updatedAt= '""" + updatedAt + """',
                        a.financialPeriod= '""" + str(replace['financialPeriod']) + """',
                        a.invoiceStatus= '""" + str(replace['invoiceStatus']) + """'
                        where
                        a.sellerGSTIN= '""" + str(replace['sellerGSTIN']) + """'
                        and a.buyerGSTIN='""" + str(replace['buyerGSTIN']) + """'
                        and a.invoiceNo='""" + str(replace['invoiceNo']) + """'
                        and a.typeofinvoice='""" + str(replace['typeofinvoice']) + """'
                        and a.invoiceFinancialPeriod='""" + data_up_eng['financial_year'] + """'"""
                        with mydb.begin() as conn:
                            conn.execute(query)
                    print("number of replace count ", count)


                mydb = DBConnectionDev()
                d_item = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + data_up_eng['financial_year'] + "'",mydb)
                # d_item = d_item.applymap(lambda x: x.strip() if type(x) == str else x)
                d_item = d_item.fillna('')

                dfff = pd.merge(left=df11, right=d_item,on=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'], how='left')
                ls1 = tuple(dfff['invoiceHeaderID'].dropna().unique())
                print(len(ls1),'number of invoice header ID to be deleted from sales invoice items table')


                if (len(ls1) > 1):
                    mydb.execute("Delete from sales_invoice_items where invoiceHeaderID in " + str(ls1) + "")
                elif (len(ls1) == 1):
                    mydb.execute("Delete from sales_invoice_items where invoiceHeaderID = " + str(ls1[0]) + "")


                item_ls = ['quantity', 'unit', 'taxableValue', 'igstRate', 'cgstRate', 'sgstOrUgstRate', 'igstAmt',
                           'cgstAmt', 'sgstOrUgstAmt', 'cessAmt', 'HSNorSAC', 'invoiceHeaderID']
                dfff = dfff[item_ls]
                print(len(dfff),'records to insert into sales invoice item table')


                dfff.to_sql(name='sales_invoice_items', con=mydb, if_exists='append', index=False, chunksize=50)


    # ###########################################################################   DATABASE DUMPING END    ###################################################################################################

    #++++++++++++++++++++++++++++++++++++++++++++++Raw data with reason and status++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                df_raw=df_raw.reset_index()
                df_raw['Reference_id'] = df_raw['id'].map(df_final_s.set_index('id')['Reference_id'])
                df_raw['reason']=df_raw['id'].map(df_final_s.set_index('id')['reason'])
                df_raw['Status']=df_raw['id'].map(df_final_s.set_index('id')['Status'])
                print(df_raw,"rawww")

    #-----------------further database work-------------------------------------------#
                time = str(datetime.datetime.now())
                filename_wout_ext= Path(data_up_eng['file']).stem
                name=str(filename_wout_ext)+'_'+time
                nn=name.replace(':','_')
                nn2=nn.replace('.','_')
                nn3=nn2+'.csv'
                # success = output.loc[output['reason']=='']
                df_raw.to_csv("/home/ubuntu/RuleBuilder/s3_fileupload/Uploaded/"+nn3)
                # Fail = output.loc[output['reason']!='']
                # Fail.to_csv("/home/ubuntu/RuleBuilder/s3_fileupload/Error_files/"+nn3)
                # output1.to_csv("/home/ubuntu/RuleBuilder/s3_fileupload/Control_Summary/"+nn3)
                #print("hereeeee")
                file_size=os.path.getsize("/home/ubuntu/RuleBuilder/s3_fileupload/Uploaded/"+nn3)
                #print(file_size,"file size path")
                file_size2=str(file_size)+" kb"
                connection=pymysql.connect(host = '52.66.30.64',user="taxgenie",password="taxgenie*#8102*$",db='taxgenie_efilling')
                cursor = connection.cursor()
                sql_insert_query ="INSERT INTO `s3_file_details_filerecord`(`file`,`filename`, `filetype`, `filename_Timestamp`, `return_type`, `Action_Status`, `row_no`, `sheet_name`, `HQId`,`sub_state`, `stock_id`, `Branch_id`, `Merge_by`, `Merge_Batch_Id`, `Deleted_by`,`Archived_by`, `processed_by`, `raw_file_path`, `processed_file_path`,`error_file_path`, `control_summary_path`, `Merge_file_path`,`Company_Id`, `type`, `financial_year`,`month`, `file_size`,`Merge_Reference`,`GSTIN`,`template_id`,status,month2) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                insert_tuple = (nn3,nn3,'.csv',nn3,'NA','Uploaded','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA','NA',data_up_eng['raw_file_path'],'NA','NA','NA','NA',data_up_eng['Company_Id'],data_up_eng['type'],data_up_eng['financial_year'], data_up_eng['month'],file_size2,'-',data_up_eng['GSTIN'],data_up_eng['template_id'],'Uploaded',data_up_eng['month2'])
                cursor.execute(sql_insert_query,insert_tuple)
                connection.commit()
                see3 = filerecord.objects.filter(filename=nn3).update(Uploadedtimestamp=time)
                see3 = filerecord.objects.filter(filename=nn3).update(Uploaded_by=time)
                see3 = filerecord.objects.filter(filename=nn3).update(Timestamp=time)
                # raw.to_csv('media3/'+file_w_ext+".csv",index=False)
                return HttpResponse(json.dumps({"status":"success","reason":"File Successfully Processed"}),status=status.HTTP_201_CREATED)
            else:
                return HttpResponse(json.dumps({"status":"failes","reason":"Please Check Your File"}),status=status.HTTP_201_CREATED)
        # elif Action_Status=='TG-FILE' and type=='GSTR2-Purchase':
        #     df_raw=pd.read_csv('/home/ubuntu/RuleBuilder/s3_fileupload/TG-FILE/'+data_up_eng['file'],encoding="ISO-8859-1")
        #     # print(df)
        #     df_raw.insert(loc=6,column='id',value=df_raw.index)
        #     print(df_raw['id'])
        #     print("uppppp")
        #     connection=pymysql.connect(host = '52.66.30.64',user = 'taxgenie',password="taxgenie*#8102*$",db='taxgenie_efilling')
        #     map1= pd.read_sql("select `Template_Name`,`column_header` from  `s3_file_details_mapping` WHERE `Company_Id`='"+data_up_eng['Company_Id']+"'and type='"+data_up_eng['type']+"' and id='"+data_up_eng['template_id']+"'",connection)
        #     print(map1,"mapping")
        #     temp_name=str(map1['Template_Name'].iloc[0])
        #     bb = map1.to_json(orient='records')
        #     kk = bb.replace("\\", '')
        #     kk1 = kk[1:-1]
        #     data = re.search(r'(?=\[)(.*\])', str(kk1)).group(1)
        #     dt = {}
        #     data1 = eval(data)
        #     for d in data1:
        #         dt.update(d)
        #     print(dt)
        #     stage_tb = pd.DataFrame()
        #     for user,sys in dt.items():
        #         stage_tb[sys]=df_raw[user]
        #     time_cal=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #     # stage_tb.insert(loc=0, column='Template_Name', value=temp_name)
        #     # stage_tb.insert(loc=1, column='Company_Name', value='Schindler')
        #     # stage_tb.insert(loc=2, column='Type', value=data_up_eng['type'])
        #     # stage_tb.insert(loc=3, column='GSTIN_No', value=data_up_eng['GSTIN'])
        #     # stage_tb.insert(loc=4, column='Financial_Year', value=data_up_eng['financial_year'])
        #     time_cal=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #     new_col=datetime.datetime.now().strftime("REFR-"+data_up_eng['GSTIN']+time_cal+data_up_eng['type'])
        #     stage_tb.insert(loc=5, column='Reference_id', value=new_col)
        #     stage_tb.insert(loc=6,column='id',value=stage_tb.index)
        #     print(stage_tb, 'raw df updated')
        #         # print(tup_ls,"list")
        #     df = Common_check_purchase(stage_tb)
        #     fail = df.loc[df['reason'] != '']
        #     Pass = df.loc[df['reason'] == '']
        #     ls = list(Pass['Invoice Type'].unique())
        #     df3 = pd.DataFrame()
        #     for i in ls:
        #         print("Inside  purchase loop")
        #         if (i.upper() == 'B2B'):
        #             b2b = Pass.loc[(Pass['Invoice Type'] == 'B2B')]
        #             # print("Lenght of the b2b",len(b2b))
        #             df2 = B2B_validation_purchase(b2b)
        #         elif (i.upper() == 'B2BUR'):
        #             b2bur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'B2BUR')]
        #             # print("Lenght of b2bur ",len(b2bur))
        #             df2 = B2BUR_validation_purchase(b2bur)
        #
        #         elif (i.upper() == 'IMPS'):
        #             imps = Pass.loc[(Pass['Invoice Type'].str.upper() == 'IMPS')]
        #             # print("Lenght of imps",len(imps))
        #             df2 = IMPS_validation_purchase(imps)
        #
        #         elif (i.upper() == 'IMPG'):
        #             impg = Pass.loc[(Pass['Invoice Type'].str.upper() == 'IMPG')]
        #             # print("Lenght of impg ", len(impg))
        #             df2 = IMPG_validation_purchase(impg)
        #
        #         elif (i.upper() == 'CNUR'):
        #             cnur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'CNUR')]
        #             # print("Lenght of cnur ",len(cnur))
        #             df2 = CDNUR_validation_purchase(cnur)
        #
        #         elif (i.upper() == 'DNUR'):
        #             dnur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'DNUR')]
        #             # print("Lenght of dnur ",len(dnur))
        #             df2 = CDNUR_validation_purchase(dnur)
        #
        #         elif (i.upper() == 'CNR'):
        #             cnr = Pass.loc[(Pass['Invoice Type'].str.upper() == 'CNR')]
        #             # print("Lenght of cnr ",len(cnr))
        #             df2 = CDNR_validation_purchase(cnr)
        #
        #         elif (i.upper() == 'DNR'):
        #             dnr = Pass.loc[(Pass['Invoice Type'].str.upper() == 'DNR')]
        #             # print("Lenght of dnr ",len(dnr))
        #             df2 = CDNR_validation_purchase(dnr)
        #
        #         df3=df3.append(df2,sort=True)
        #
        #     df_final=fail.append(df3, sort=True)
        #     df_final_s = pd.DataFrame()
        #
        #     df_final_s=df_final_s.append(df_final, sort=True)
        #     #del df_final['level_0']
        #     # df_final = df_final.reset_index()
        #
        #
        # print(df_final.columns)
        # if 'index' in df_final.columns.tolist():
        #     print('index')
        #     del df_final['index']
        # if 'level_0' in df_final.columns.tolist():
        #     print('level 0')
        #     del df_final['level_0']
        # if 'Reason' in df_final.columns.tolist():
        #     print('Reason')
        #     del df_final['Reason']
        # df_final['Status'] = np.where((df_final['reason'] == ''), 'Success', 'Fail')
        # df_final['invoiceFinancialPeriod'] = data_up_eng['financial_year']
        #
        # df_final.astype(str).to_sql(name='purchase_result_table', con=mydb, if_exists='append', index=False,chunksize=1000)
        # print(df_final,'Final data dump in result table for purchase')
        # # df_success=df_final.loc[df_final['Status']=='Success']
        #
        # # ###################################DataBase connection###################################################################################################
        #
        # mydb = DBConnection()
        #
        # df11 = pd.read_sql("SELECT * FROM purchase_result_table where Reference_id like '%%"+pan_num+"%%"+time_cal+data_up_eng['type']+"' and status='Success' ",mydb)
        # df11.rename(columns={
        #     'Buyer Gstin': 'buyerGSTIN',
        #     'Invoice Type': 'typeofinvoice',
        #     'Invoice SubType': 'invoiceSubType',
        #     'Invoice Date': 'invoiceDate',
        #     'Invoice No': 'invoiceNo',
        #     'Invoice Value': 'invoiceValue',
        #     'quantity': 'quantity',
        #     'Reverse Charge': 'reverseCharge',
        #     'Seller Gstin': 'sellerGSTIN',
        #     'unit': 'unit',
        #     'Place Of Supply': 'pos',
        #     'Taxable Value': 'taxableValue',
        #     'Igst Rate': 'igstRate',
        #     'Cgst Rate': 'cgstRate',
        #     'Sgst Rate': 'sgstOrUgstRate',
        #     'Igst Amount': 'igstAmt',
        #     'Cgst Amount': 'cgstAmt',
        #     'Sgst Amount': 'sgstOrUgstAmt',
        #     'Cess Amount': 'cessAmt',
        #     'Hsn Code': 'HSNorSAC',
        #     'Rate': 'gstRate',
        #     'invoiceFinancialPeriod': 'invoiceFinancialPeriod',
        #     'Reference_id': 'Reference_id'}, inplace=True)
        #
        # df11['sellerGSTIN'] = df11['sellerGSTIN'].astype(str).str.replace('nan', '')
        # print(df11, 'df after renaming from result table')
        #
        # header_ls = ['buyerGSTIN', 'typeofinvoice', 'invoiceSubType', 'invoiceDate', 'invoiceNo',
        #              'invoiceValue', 'sellerGSTIN', 'pos', 'reverseCharge', 'Reference_id',
        #              'invoiceFinancialPeriod']
        #
        # d = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" +data_up_eng['financial_year'] + "'", mydb)
        # # d = d.applymap(lambda x: x.strip() if type(x) == str else x)
        # print(d, "data from purchase invoice header")
        # group = df11.groupby(['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'])
        # count = 1
        # for name, gr in group:
        #     print(count,"=======================================>>>>>>>>>>>>COUNT<<<<<<<<<<<====================================================")
        #     print("inside for")
        #     s = len(gr)
        #     # gr.to_csv("C:/Users/Admin/Desktop/testpurchase.csv")
        #
        #     # ++++++++++++++++++++++++++++Adding invoice header and item code++++++++++++++++++++++++++++++++++
        #     df1 = gr[header_ls].drop_duplicates(subset=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'], keep='first')
        #
        #     replace = df1.loc[((df1['sellerGSTIN'].isin(d['sellerGSTIN'])) & (df1['invoiceNo'].isin(d['invoiceNo'])) & (df1['typeofinvoice'].isin(d['typeofinvoice'])) & (df1['buyerGSTIN'].isin(d['buyerGSTIN'])) & (df1['invoiceFinancialPeriod'].isin(d['invoiceFinancialPeriod'])))]
        #     insert = df1[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN', 'invoiceDate', 'reverseCharge','pos', 'invoiceFinancialPeriod']].loc[~((df1['sellerGSTIN'].isin(d['sellerGSTIN'])) & (df1['invoiceNo'].isin(d['invoiceNo'])) & (df1['typeofinvoice'].isin(d['typeofinvoice'])) & (df1['buyerGSTIN'].isin(d['buyerGSTIN'])))]
        #
        #     insert.to_sql('purchase_invoice_header', mydb, if_exists='append', index=False)
        #     replace.to_sql('purchase_header_demo', mydb, if_exists='replace', index=False)
        #
        #     print(insert, "data to insert purchase")
        #     print(replace, "data to replace purchase")
        #
        #     # +++++++++++++++++++++++++++++++++Updated At+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        #
        #     updatedAt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #     query = """update sales_invoice_header a inner join header_demo  b
        #                    on  a.sellerGSTIN=b.sellerGSTIN
        #                    and a.invoiceNo=b.invoiceNo and
        #                    a.typeofinvoice=b.typeofinvoice and
        #                     a.buyerGSTIN=b.buyerGSTIN
        #                    set
        #                    a.invoiceSubType=b.invoiceSubType,
        #                    a.invoiceDate = b.invoiceDate,
        #                    a.reverseCharge = b.reverseCharge,
        #                    a.invoiceFinancialPeriod = b.invoiceFinancialPeriod,
        #                    a.pos= b.pos,
        #                    a.updatedAt = '""" + updatedAt + """'
        #                    where a.sellerGSTIN  like '%%""" + pan_num + """%%' and
        #                    a.invoiceFinancialPeriod='""" + data_up_eng['financial_year'] + """' """
        #     with mydb.begin() as conn:
        #         conn.execute(query)
        #     print("update query executed")
        #
        #     d_item = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" +data_up_eng['financial_year'] + "'", mydb)
        #     # d_item = d_item.applymap(lambda x: x.strip() if type(x) == str else x)
        #     d_item = d_item.fillna('')
        #     print(gr.columns,"grr")
        #     dfff = pd.merge(left=gr, right=d_item,on=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'], how='left')
        #     print(dfff.columns, "df after merging")
        #
        #     ls1 = tuple(dfff['invoiceHeaderID'].dropna().unique())
        #     print(len(ls1), "list to update and delete")
        #     print(ls1, "list to delete and insert")
        #     if (len(ls1)!=0):
        #         if (len(ls1) > 1):
        #             print("sfsfdf")
        #             mydb.execute("Delete from purchase_invoice_items where invoiceHeaderID in " + str(ls1) + "")
        #         else:
        #             mydb.execute("Delete from purchase_invoice_items where invoiceHeaderID = " + str(ls1[0]) + "")
        #
        #     item_ls = ['taxableValue', 'igstRate', 'cgstRate', 'sgstOrUgstRate',
        #                'igstAmt', 'cgstAmt', 'sgstOrUgstAmt', 'invoiceHeaderID']
        #     dfff = dfff[item_ls]
        #     print("++++++++++++++++++", dfff.columns)
        #     dfff.to_sql(name='purchase_invoice_items', con=mydb, if_exists='append', index=False, chunksize=1000)
        #     print("Dumping done", dfff['invoiceHeaderID'])
        #     count += 1
        #
        # # ###################################DataBase connection###################################################################################################
        #
        # # ++++++++++++++++++++++++++++++++++++++++++++++Raw data with reason and status++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        # df_raw = df_raw.reset_index()
        # #raw.rename(columns={raw.iloc[0][1]: 'Reference_id'}, inplace=True)
        # df_raw['Reference_id'] = df_raw['id'].map(df_final.set_index('id')['Reference_id'])
        # df_raw['reason'] = df_raw['id'].map(df_final.set_index('id')['reason'])
        # df_raw['Status'] = df_raw['id'].map(df_final.set_index('id')['Status'])
        # # df_raw.to_csv('E:/PYTHON PROJECTS/MAPPER UPLOAD/Upload_Mapper_project/media3/' + file_w_ext + ".csv")
        # return HttpResponse(json.dumps({"status": "success"}), status=status.HTTP_201_CREATED)



class Mappingview(APIView):
    def post(self,request):
        temp_name=request.data['Template_Name']
        comp_id=request.data['Company_Id']
        type=request.data['type']
        see = list(Mapping.objects.filter(Template_Name=temp_name).values())
        print(see)
        dtaat=see[-1]
        a=dtaat['column_header']
        print(a)
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
