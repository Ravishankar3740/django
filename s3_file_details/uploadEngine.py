from .views import *
from .Sales_validation import *
from .purchase_validation import *
import pymysql, re, json
from datetime import datetime
import pandas as pd
import numpy as np
from django.shortcuts import HttpResponse
from rest_framework.views import APIView

class upload_engine(APIView):
    def post(self,request):
        dataUploadEngine = request.data['data']
        try:
            con = pymysql.connect(host="15.206.93.178", user="taxgenie", password="taxgenie*#8102*$",db='taxgenie_efilling')
            map1 = pd.read_sql("select column_header from s3_file_details_mapping where id ='" + dataUploadEngine['template_id'] + "'",con)
        except Exception as e:
            print(str(e),'Database exception')
            return HttpResponse(json.dumps({"reason": "Cannot Fetch Data try again later or contact service provider", "final_status":"Connection Error"}))

        bb = map1.to_json(orient='records').replace("\\", '')[1:-1]
        data = re.search(r'(?=\[)(.*\])', str(bb)).group(1)
        dt = {}
        for d in eval(data):
            dt.update(d)

        customerGSTIN = dataUploadEngine['GSTIN']
        typeData = dataUploadEngine['type']
        pan_num = customerGSTIN[2:-3]
        timeStamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # TIMESTAMP FOR REFERENCE ID

        if dataUploadEngine['Action_Status']=='TG-FILE' and typeData=='GSTR1-Sales':
            try:
                raw=pd.read_csv("C:/Users/Admin/PycharmProjects/s3_fileupload 12-11-2019/s3_fileupload/TG-FILE/"+dataUploadEngine['filename'])
            except Exception as e:
                print(str(e))
                return HttpResponse(json.dumps({'reason' : str(e),'final_status' : 'FAIL'}))
            if (len(raw) == 0):
                return HttpResponse(json.dumps({"reason": "No Records passed TG validation. Check the error file.", "final_status": "FAIL"}))
            else:
                raw.insert(loc=6, column='id', value=raw.index)
                #####################   Column Mapping  #####################################################
                stage_tb = pd.DataFrame()
                for user, sys in dt.items():
                    stage_tb[sys] = raw[user]
                stage_tb['id'] = raw['id']
                print(len(stage_tb),'data to process')
                ##############################################################################################

                ls = stage_tb["Seller Gstin"].unique().tolist()
                print(ls)
                df_final_s = pd.DataFrame()
                for j in ls:
                    df_f = stage_tb.loc[stage_tb['Seller Gstin'] == j]
                    df_f['reason'] = ''
                    try:
                        sellerID = pd.read_sql("select companyID from company_master where GSTNINNO  ='" + j + "'", con)
                    except Exception as e:
                        print(str(e))
                        return HttpResponse(json.dumps({"reason": "Cannot Fetch Data try again later or contact your service provider","final_status": "Connection Error"}))

                    if (len(sellerID) != 0):
                        df_f['sellerID'] = sellerID['companyID'].loc[0]
                    else:
                        df_f['sellerID'] = ''

                    refID = datetime.now().strftime("REFR-" + j + timeStamp + typeData)
                    df_f['Reference_id'] = refID
                    df_f = df_f.reset_index()
                    df3 = pd.DataFrame()
                    df21 = pd.DataFrame()
                    df31 = pd.DataFrame()
                    if ((j[2:-3] == pan_num) and (len(sellerID) != 0)):
                        print("PAN match" ,j)
                        print("PAN match from front", pan_num)
                        # df_f.to_csv(r"C:\Users\Admin\Desktop\data in df_f.csv")
                        df = Common_check(df_f)
    ############################################# AUTO POPULATING FINANCIAL PERIOD and YEAR##########################################################
                        df.loc[((df['Invoice Date'].dt.month) <= 4), 'invoiceFinancialPeriod'] = ((df['Invoice Date'].dt.year - 1).fillna(0).astype(int)).map(str) + "-" + ((df['Invoice Date'].dt.year).fillna(0).astype(int)).map(str)
                        df.loc[((df['Invoice Date'].dt.month) > 4), 'invoiceFinancialPeriod'] = ((df['Invoice Date'].dt.year).fillna(0).astype(int)).map(str) + "-" + ((df['Invoice Date'].dt.year + 1).fillna(0).astype(int)).map(str)
                        df['financialPeriod'] = dataUploadEngine['month2']
    ##################################################################################################################################################
                        # df.to_csv(r"C:\Users\Admin\Desktop\data from common check.csv")
                        fail = df.loc[df['reason'] != '']
                        # fail.to_csv(r"C:\Users\Admin\Desktop\fail dataframe.csv")
                        Pass = df.loc[df['reason'] == '']

                        # Pass.to_csv(r"C:\Users\Admin\Desktop\Pass dataframe.csv")

                        ls12 = list(Pass['Invoice Type'].unique())
                        print(ls12,'loop based on invoice type')

                        for i in ls12:
                            if (i.upper() == 'B2B'):
                                b2b = Pass.loc[(Pass['Invoice Type'] == 'B2B')]
                                df2 = B2B_validation(b2b)
                            elif (i.upper() == 'B2CS'):
                                b2c = Pass.loc[(Pass['Invoice Type'].str.upper() == 'B2CS')]
                                df2 = B2C_validation(b2c)
                            elif (i.upper() == 'B2CL'):
                                b2c = Pass.loc[(Pass['Invoice Type'].str.upper() == 'B2CL')]
                                df2 = B2C_validation(b2c)
                            elif (i.upper() == 'CNUR'):
                                cdnur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'CNUR')]
                                df2 = CDNUR_validation(cdnur)
                            elif (i.upper() == 'DNUR'):
                                cdnur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'DNUR')]
                                df2 = CDNUR_validation(cdnur)
                            elif (i.upper() == 'CNR'):
                                cdnr = Pass.loc[(Pass['Invoice Type'].str.upper() == 'CNR')]
                                df2 = CDNR_validation(cdnr)
                            elif (i.upper() == 'DNR'):
                                cdnr = Pass.loc[(Pass['Invoice Type'].str.upper() == 'DNR')]
                                df2 = CDNR_validation(cdnr)
                            elif (i.upper() == 'EXP'):
                                exp = Pass.loc[(Pass['Invoice Type'].str.upper() == 'EXP')]
                                df2 = Export_validation(exp)
                            df21 = df21.append(df2, sort=True)

                        df3 = df3.append([df21,fail], sort=True)
                        # df3.to_csv(r"C:\Users\Admin\Desktop\df3 dataframe.csv")

                    else:
                        if ((j[2:-3] != pan_num) and (len(sellerID) == 0)):
                            print("I am 1st condition")
                            print("PAN match", j)
                            print("PAN match from front", pan_num)
                            df_f.loc[df_f['sellerID'] == '', 'reason'] = df_f['reason'] + 'GSTIN is not registered with TG , Invalid PAN number for GSTIN'
                            df31 = df_f

                        elif (len(sellerID) == 0):
                            print("I am 2nd condition")
                            print("PAN match", j)
                            print("PAN match from front", pan_num)
                            df_f.loc[df_f['sellerID'] == '', 'reason'] = df_f['reason'] + 'GSTIN is not registered with TG'
                            df31 = df_f

                        else:
                            print(" I am in else condtion")
                            print("PAN match", j)
                            print("PAN match from front", pan_num)
                            df_f['reason'] = df_f['reason'] + 'Invalid PAN number for GSTIN'
                            df31 = df_f

                    # df_final = fail.append([df3, df31], sort=True)
                    # df31.to_csv(r"C:\Users\Admin\Desktop\df31 dataframe.csv")
                    df_final_s = df_final_s.append([df3, df31], sort=True)

                if 'index' in df_final_s.columns.tolist():
                    del df_final_s['index']
                if 'level_0' in df_final_s.columns.tolist():
                    del df_final_s['level_0']
                if 'Reason' in df_final_s.columns.tolist():
                    del df_final_s['Reason']

                # df_final_s.to_csv(r"C:\Users\Admin\Desktop\df_final_s after validation.csv")
                print(df_final_s.columns,'columns')
                df_final_s['Status'] = np.where((df_final_s['reason'] == ''), 'Success', 'Fail')
                df_final_s['gstnStatus'] ='notuploaded'
                df_final_s['invoiceStatus'] = 'Y'
                print(df_final_s, 'final data after validation')
                # df_final_s.to_csv(r"C:\Users\Admin\Desktop\df_final_s after validation atfer updation.csv")

                pan_check = df_final_s.loc[df_final_s['Status'] == 'Success']
                print(pan_check, 'pan check success data')
                if (len(pan_check)==0):
                    return HttpResponse(json.dumps({"reason": "PAN VALIDATION FAILED","final_status": "Fail"}))
                else:
                    try:
                        df_final_s.astype(str).to_sql(name='upload_sales_result_table', con=mydb, if_exists='append', index=False,chunksize=500)
                    except Exception as e:
                        print(str(e))
                        return HttpResponse(json.dumps({"reason": str(e),"final_status": "Connection Error"}))
        ######################################################################   DATABASE DUMPING START  ###################################################################################################
                    print("##################################################### DB DUMPING START #################################################################################")
                    dataSuccess = pd.read_sql("SELECT * FROM upload_sales_result_table where Reference_id like '%%" + pan_num + "%%" + timeStamp + typeData + "' and Status='Success' ",mydb)
                    print("SELECT * FROM upload_sales_result_table where Reference_id like '%%" + pan_num + "%%" + timeStamp + typeData + "' and Status='Success' ")
                    print(len(dataSuccess), "Success data from result sales table")

                    if(len(dataSuccess) == 0):
                        return HttpResponse(json.dumps({"reason": "All Invoice Records with status Fail", "final_status": "FAIL"}))
                    else:
                        dataSuccess.rename(columns={
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

                        print("financialperiod unique ----------------------------------> ", dataSuccess['financialPeriod'].unique())
                        process = str(dataSuccess['invoiceFinancialPeriod'].iloc[0])
                        print(tuple(dataSuccess['invoiceFinancialPeriod'].unique()))
                        print(process,'data in process')
                        # dataSuccess.to_csv(r"C:\Users\Admin\Desktop\financial.csv")

                        # if(len(process) == 1):
                        dataSalesHeader = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
                        # elif(len(process) > 1):
                        #     dataSalesHeader = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod in " + str(process) + "",mydb)
                        dataSalesHeader = dataSalesHeader.applymap(lambda x: x.strip() if type(x) == str else x)
                        dataSalesHeader = dataSalesHeader.fillna('')
                        dataSuccess = dataSuccess.fillna('')

                        header_ls = ['buyerGSTIN', 'typeofinvoice', 'invoiceSubType', 'invoiceDate', 'invoiceNo',
                                     'invoiceValue', 'sellerGSTIN', 'pos', 'reverseCharge', 'Reference_id',
                                     'invoiceFinancialPeriod', 'sellerID', 'financialPeriod', 'gstnStatus', 'invoiceStatus']
                        dataSuccess['buyerGSTIN'] = dataSuccess['buyerGSTIN'].astype(str).str.replace('nan', '')

                        dataUnique = dataSuccess[header_ls].drop_duplicates(subset=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'],keep='first')
                        dataReplace = dataUnique.loc[((dataUnique['sellerGSTIN'].isin(dataSalesHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataSalesHeader['invoiceNo']))& (dataUnique['typeofinvoice'].isin(dataSalesHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataSalesHeader['buyerGSTIN']))& (dataUnique['invoiceFinancialPeriod'].isin(dataSalesHeader['invoiceFinancialPeriod'])))]
                        print(len(dataReplace),'data to replace')

                        dataInsert = dataUnique[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'invoiceSubType', 'gstnStatus', 'sellerID','invoiceStatus', 'financialPeriod', 'buyerGSTIN', 'invoiceDate', 'reverseCharge', 'pos','invoiceFinancialPeriod']].loc[~((dataUnique['sellerGSTIN'].isin(dataSalesHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataSalesHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataSalesHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataSalesHeader['buyerGSTIN'])))]
                        print(len(dataInsert),'data to insert')

                        if (len(dataInsert) != 0):
                            dataInsert.to_sql('sales_invoice_header', mydb, if_exists='append', index=False, chunksize=100)

                        if (len(dataReplace) != 0):
                            print("INSIDE UPDATE")
                            count = 0
                            for index, replace in dataReplace.iterrows():
                                count = count + 1
                                updatedAt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
                                and a.invoiceFinancialPeriod='""" + str(replace['invoiceFinancialPeriod']) + """'
                                and a.sellerGSTIN like '%%""" + pan_num + """%%' """
                                with mydb.begin() as conn:
                                    conn.execute(query)
                            print("number of replace count ", count)

                        dataSalesItem = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
                        print("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM sales_invoice_header where sellerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'")
                        dataSalesItem = dataSalesItem.applymap(lambda x: x.strip() if type(x) == str else x)
                        dataSalesItem = dataSalesItem.fillna('')
                        print(len(dataSalesItem), 'data from invoice header for item table df creation')
                        # dataSalesItem.to_csv(r"C:\Users\Admin\Desktop\Data from sales invoice header for item table.csv")
                        # dataSuccess.to_csv(r"C:\Users\Admin\Desktop\Data from result table.csv")

                        finalDataSalesItem = pd.merge(left=dataSuccess, right=dataSalesItem, on=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'],how='left')
                        # finalDataSalesItem.to_csv(r"C:\Users\Admin\Desktop\Data after merging.csv")
                        ls1 = tuple(finalDataSalesItem['invoiceHeaderID'].dropna().unique())
                        print(len(finalDataSalesItem), 'data after merging for item table')
                        print(len(ls1), 'number of invoice header ID to be deleted from sales invoice items table')

                        if (len(ls1) > 1):
                            mydb.execute("Delete from sales_invoice_items where invoiceHeaderID in " + str(ls1) + "")
                        elif (len(ls1) == 1):
                            mydb.execute("Delete from sales_invoice_items where invoiceHeaderID = " + str(ls1[0]) + "")

                        item_ls = ['quantity', 'unit', 'taxableValue', 'igstRate', 'cgstRate', 'sgstOrUgstRate', 'igstAmt',
                                   'cgstAmt', 'sgstOrUgstAmt', 'cessAmt', 'HSNorSAC', 'invoiceHeaderID']
                        finalDataSalesItem = finalDataSalesItem[item_ls]
                        finalDataSalesItem['srNo'] = ''
                        finalDataSalesItem['srNo'] = finalDataSalesItem.groupby('invoiceHeaderID').cumcount() + 1
                        print(len(finalDataSalesItem), 'records to insert into sales invoice item table')

                        finalDataSalesItem.to_sql(name='sales_invoice_items', con=mydb, if_exists='append', index=False, chunksize=50)

                        raw = raw.reset_index()
                        raw['Reference_id'] = raw['id'].map(df_final_s.set_index('id')['Reference_id'])
                        raw['reason'] = raw['id'].map(df_final_s.set_index('id')['reason'])
                        raw['Status'] = raw['id'].map(df_final_s.set_index('id')['Status'])

                        return HttpResponse(json.dumps({"reason":"Sales upload successful","final_status":"SUCCESSS"}), status=status.HTTP_201_CREATED)

#############################################   PURCHASE PART  ##############################################################################

        elif dataUploadEngine['Action_Status'] == 'TG-FILE' and typeData == 'GSTR2-Purchase':
            try:
                raw = pd.read_csv("C:/Users/Admin/PycharmProjects/s3_fileupload 12-11-2019/s3_fileupload/TG-FILE/" + dataUploadEngine['filename'])
            except Exception as e:
                print(str(e))
                return HttpResponse(json.dumps({'reason': str(e), 'final_status': 'FAIL'}))
            if (len(raw) == 0):
                return HttpResponse(json.dumps({"reason": "No Records passed TG validation", "final_status": "FAIL"}))
            else:
                raw.insert(loc=6, column='id', value=raw.index)
                #####################   Column Mapping  #####################################################
                stage_tb = pd.DataFrame()
                for user, sys in dt.items():
                    stage_tb[sys] = raw[user]
                stage_tb['id'] = raw['id']
                print(len(stage_tb), 'data to process')
                ##############################################################################################

                ls = stage_tb["Buyer Gstin"].unique().tolist()
                # print(ls)
                df_final_s = pd.DataFrame()
                for j in ls:
                    # print(j,'Buyer GSTIN')
                    df_f = stage_tb.loc[stage_tb['Buyer Gstin'] == j]
                    df_f['reason'] = ''
                    buyerID = pd.read_sql("select companyID from company_master where GSTNINNO  ='" + j + "'", con)

                    if (len(buyerID) != 0):
                        df_f['buyerID'] = buyerID['companyID'].loc[0]
                    else:
                        df_f['buyerID'] = ''

                    refID = datetime.now().strftime("REFR-" + j + timeStamp + typeData)
                    df_f['Reference_id'] = refID
                    df_f = df_f.reset_index()
                    if 'level_0' in df_f.columns.tolist():
                        del df_f['level_0']
                    df_f = df_f.reset_index()
                    df3 = pd.DataFrame()
                    df21 = pd.DataFrame()
                    df31 = pd.DataFrame()
                    if ((j[2:-3] == pan_num) and (len(buyerID) != 0)):
                        print("PAN match",j[2:-3])
                        print("PAN from Angular", pan_num)
                        df = Common_check_purchase(df_f)
                        ############################################# AUTO POPULATING FINANCIAL PERIOD and YEAR##########################################################
                        df.loc[((df['Invoice Date'].dt.month) <= 4), 'invoiceFinancialPeriod'] = ((df['Invoice Date'].dt.year - 1).fillna(0).astype(int)).map(str) + "-" + ((df['Invoice Date'].dt.year).fillna(0).astype(int)).map(str)
                        df.loc[((df['Invoice Date'].dt.month) > 4), 'invoiceFinancialPeriod'] = ((df['Invoice Date'].dt.year).fillna(0).astype(int)).map(str) + "-" + ((df['Invoice Date'].dt.year + 1).fillna(0).astype(int)).map(str)
                        df['financialPeriod'] = dataUploadEngine['month2']
                        ##################################################################################################################################################
                        fail = df.loc[df['reason'] != '']
                        Pass = df.loc[df['reason'] == '']
                        ls12 = list(Pass['Invoice Type'].unique())

                        for i in ls12:
                            if (i.upper() == 'B2B'):
                                b2b = Pass.loc[(Pass['Invoice Type'] == 'B2B')]
                                df2 = B2B_validation_purchase(b2b)
                            elif (i.upper() == 'B2BUR'):
                                b2bur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'B2BUR')]
                                df2 = B2BUR_validation_purchase(b2bur)
                            elif (i.upper() == 'IMPS'):
                                imps = Pass.loc[(Pass['Invoice Type'].str.upper() == 'IMPS')]
                                df2 = IMPS_validation_purchase(imps)
                            elif (i.upper() == 'IMPG'):
                                impg = Pass.loc[(Pass['Invoice Type'].str.upper() == 'IMPG')]
                                df2 = IMPG_validation_purchase(impg)
                            elif (i.upper() == 'CNUR'):
                                cnur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'CNUR')]
                                df2 = CDNUR_validation_purchase(cnur)
                            elif (i.upper() == 'DNUR'):
                                dnur = Pass.loc[(Pass['Invoice Type'].str.upper() == 'DNUR')]
                                df2 = CDNUR_validation_purchase(dnur)
                            elif (i.upper() == 'CNR'):
                                cnr = Pass.loc[(Pass['Invoice Type'].str.upper() == 'CNR')]
                                df2 = CDNR_validation_purchase(cnr)
                            elif (i.upper() == 'DNR'):
                                dnr = Pass.loc[(Pass['Invoice Type'].str.upper() == 'DNR')]
                                df2 = CDNR_validation_purchase(dnr)
                            df21 = df21.append(df2, sort=True)

                        df3 = df3.append([df21, fail], sort=True)
                    else:
                        if ((j[2:-3] != pan_num) and (len(buyerID) == 0)):
                            print("I am 1st condition",j)
                            print("PAN from Angular", pan_num)
                            df_f.loc[df_f['buyerID'] == '', 'reason'] = df_f['reason'] + 'GSTIN is not registered with TG , Invalid PAN number for GSTIN'
                            df31 = df_f

                        elif (len(buyerID) == 0):
                            print("I am 2nd condition",j)
                            print("PAN from Angular", pan_num)
                            df_f.loc[df_f['buyerID'] == '', 'reason'] = df_f['reason'] + 'GSTIN is not registered with TG'
                            df31 = df_f

                        else:
                            print(" I am in else condtion",j)
                            print("PAN from Angular", pan_num)
                            df_f['reason'] = df_f['reason'] + 'Invalid PAN number for GSTIN'
                            df31 = df_f

                    df_final_s = df_final_s.append([df3, df31], sort=True)

                if 'index' in df_final_s.columns.tolist():
                    del df_final_s['index']
                if 'level_0' in df_final_s.columns.tolist():
                    del df_final_s['level_0']
                if 'Reason' in df_final_s.columns.tolist():
                    del df_final_s['Reason']

                df_final_s['Status'] = np.where((df_final_s['reason'] == ''), 'Success', 'Fail')
                df_final_s['gstnStatus'] = 'notuploaded'
                df_final_s['invoiceStatus'] = 'Y'
                print(df_final_s, 'final data after validation')

                pan_check = df_final_s.loc[df_final_s['Status'] == 'Success']
                print(pan_check,'pan check success data')
                if (len(pan_check) == 0):
                    return HttpResponse(json.dumps({"reason": "PAN VALIDATION FAILED", "final_status": "Fail"}))
                else:
                    try:
                        df_final_s.astype(str).to_sql(name='upload_purchase_result_table', con=mydb, if_exists='append',index=False, chunksize=500)
                    except Exception as e:
                        print(str(e))
                        return HttpResponse(json.dumps({"reason": str(e), "final_status": "Connection Error"}))
                ######################################################################   DATABASE DUMPING START  ###################################################################################################
                    print("##################################################### DB DUMPING START #################################################################################")
                    dataSuccess = pd.read_sql("SELECT * FROM upload_purchase_result_table where Reference_id like '%%" + pan_num + "%%" + timeStamp + typeData + "' and Status='Success' ",mydb)
                    print("SELECT * FROM upload_purchase_result_table where Reference_id like '%%" + pan_num + "%%" + timeStamp + typeData + "' and Status='Success' ")
                    print(len(dataSuccess), "Success data from result purchase table")

                    if (len(dataSuccess) == 0):
                        return HttpResponse(json.dumps({"reason": "All Invoice Records with status Fail", "final_status": "FAIL"}))
                    else:
                        dataSuccess.rename(columns={
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

                        print("financialperiod unique ----------------------------------> ",dataSuccess['financialPeriod'].unique())
                        process = str(dataSuccess['invoiceFinancialPeriod'].iloc[0])
                        print(tuple(dataSuccess['invoiceFinancialPeriod'].unique()))
                        print(process, 'data in process')

                        dataPurchaseHeader = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID,invoiceFinancialPeriod FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
                        dataPurchaseHeader = dataPurchaseHeader.applymap(lambda x: x.strip() if type(x) == str else x)
                        dataPurchaseHeader = dataPurchaseHeader.fillna('')
                        dataSuccess = dataSuccess.fillna('')

                        header_ls = ['buyerGSTIN', 'typeofinvoice', 'invoiceSubType', 'invoiceDate', 'invoiceNo',
                                     'invoiceValue', 'sellerGSTIN', 'pos', 'reverseCharge', 'Reference_id',
                                     'invoiceFinancialPeriod', 'buyerID', 'financialPeriod', 'gstnStatus',
                                     'invoiceStatus']
                        dataSuccess['sellerGSTIN'] = dataSuccess['sellerGSTIN'].astype(str).str.replace('nan', '')

                        dataUnique = dataSuccess[header_ls].drop_duplicates(subset=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'], keep='first')
                        dataReplace = dataUnique.loc[((dataUnique['sellerGSTIN'].isin(dataPurchaseHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataPurchaseHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataPurchaseHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataPurchaseHeader['buyerGSTIN'])) & (dataUnique['invoiceFinancialPeriod'].isin(dataPurchaseHeader['invoiceFinancialPeriod'])))]
                        print(dataReplace, 'data to replace')

                        dataInsert = dataUnique[['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'invoiceSubType', 'gstnStatus', 'buyerID','invoiceStatus', 'financialPeriod', 'buyerGSTIN', 'invoiceDate', 'reverseCharge', 'pos','invoiceFinancialPeriod']].loc[~((dataUnique['sellerGSTIN'].isin(dataPurchaseHeader['sellerGSTIN'])) & (dataUnique['invoiceNo'].isin(dataPurchaseHeader['invoiceNo'])) & (dataUnique['typeofinvoice'].isin(dataPurchaseHeader['typeofinvoice'])) & (dataUnique['buyerGSTIN'].isin(dataPurchaseHeader['buyerGSTIN'])))]
                        print(dataInsert, 'data to insert')

                        if (len(dataInsert) != 0):
                            dataInsert.to_sql('purchase_invoice_header', mydb, if_exists='append', index=False,chunksize=100)

                        if (len(dataReplace) != 0):
                            print("INSIDE UPDATE")
                            count = 0
                            for index, replace in dataReplace.iterrows():
                                count = count + 1
                                updatedAt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                query = """update purchase_invoice_header a
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
                                and a.invoiceFinancialPeriod='""" + str(replace['invoiceFinancialPeriod']) + """'
                                and a.buyerGSTIN like '%%""" + pan_num + """%%' """
                                with mydb.begin() as conn:
                                    conn.execute(query)
                            print("number of replace count ", count)

                        dataPurchaseItem = pd.read_sql("SELECT sellerGSTIN,invoiceNo,typeofinvoice,buyerGSTIN,invoiceHeaderID FROM purchase_invoice_header where buyerGSTIN  like '%%" + pan_num + "%%' and invoiceFinancialPeriod='" + process + "'",mydb)
                        dataPurchaseItem = dataPurchaseItem.applymap(lambda x: x.strip() if type(x) == str else x)
                        dataPurchaseItem = dataPurchaseItem.fillna('')
                        print(dataPurchaseItem, 'data from invoice header for item table df creation')

                        finalDataPurchaseItem = pd.merge(left=dataSuccess, right=dataPurchaseItem,on=['sellerGSTIN', 'invoiceNo', 'typeofinvoice', 'buyerGSTIN'],how='left')
                        ls1 = tuple(finalDataPurchaseItem['invoiceHeaderID'].dropna().unique())
                        print(finalDataPurchaseItem, 'data after merging for item table')
                        print(len(ls1), 'number of invoice header ID to be deleted from purchase invoice items table')

                        if (len(ls1) > 1):
                            mydb.execute("Delete from purchase_invoice_items where invoiceHeaderID in " + str(ls1) + "")
                        elif (len(ls1) == 1):
                            mydb.execute("Delete from purchase_invoice_items where invoiceHeaderID = " + str(ls1[0]) + "")

                        item_ls = ['taxableValue', 'igstRate', 'cgstRate',
                                   'sgstOrUgstRate','igstAmt', 'cgstAmt',
                                   'sgstOrUgstAmt', 'cessAmt', 'HSNorSAC', 'invoiceHeaderID']
                        finalDataPurchaseItem = finalDataPurchaseItem[item_ls]
                        finalDataPurchaseItem['srNo'] = ''
                        finalDataPurchaseItem['srNo'] = finalDataPurchaseItem.groupby('invoiceHeaderID').cumcount() + 1
                        print(len(finalDataPurchaseItem), 'records to insert into purchase invoice item table')

                        finalDataPurchaseItem.to_sql(name='purchase_invoice_items', con=mydb, if_exists='append', index=False,chunksize=50)

                        raw = raw.reset_index()
                        raw['Reference_id'] = raw['id'].map(df_final_s.set_index('id')['Reference_id'])
                        raw['reason'] = raw['id'].map(df_final_s.set_index('id')['reason'])
                        raw['Status'] = raw['id'].map(df_final_s.set_index('id')['Status'])

                        return HttpResponse(json.dumps({"reason": "PURCHASE UPLOAD SUCCESSSFUL", "final_status": "SUCCESS"}),status=status.HTTP_201_CREATED)
        else:
            return HttpResponse(json.dumps({"reason": "Condition not satisfied for sales or purchase.", "final_status": "FAIL"}))
