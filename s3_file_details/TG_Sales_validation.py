import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
from datetime import datetime
import re

class TG_validation:
    def Sales(self,stage_tb, pan_num):
        state_master_dict = {
            '01': ['jammu and kashmir', 'jammu & kashmir', 'jammu and kashmir', 'j&k', 'jk', '01-jammu & kashmir',
                   '01-jammu and kashmir', '01', '1', 1],
            '02': ['himachal pradesh', 'himachal pradesh', 'hp', '02-himachal pradesh', '02', '2', 2],
            '03': ['punjab', 'panjab', 'pb', '03-punjab', '03', '3', 3],
            '04': ['chandigarh', 'chandigarh', '04-chandigarh', 'ch', '04', '4', 4],
            '05': ['uttarakhand', 'uttarakhand', 'uk', '05-uttarakhand', '05', '5', 5],
            '06': ['haryana', 'haryana', 'hr', '06-haryana', '06', '6', 6],
            '07': ['delhi', 'delhi', 'dl', 'new delhi', 'new-delhi', 'newdelhi', 'new_delhi', 'dl', '07-delhi',
                   '07-new delhi',
                   '07-new-delhi', '07', '7', 7],
            '08': ['rajasthan', 'rajsthan', 'rj', 'rajasthan', '08-rajasthan', '08', '8', 8],
            '09': ['uttar pradesh', 'uttar-pradesh', 'uttar_pradesh', 'up', 'utar pradesh', '09-punjab', '09', '9', 9],
            '10': ['bihar', 'bhihar', 'br', '10-bihar', '10', 10],
            '11': ['sikkim', 'sikkhim', 'sikkim', 'sk', '11-sikkim', '11', 11],
            '12': ['arunachal pradesh', 'arunachal-pradesh', 'arunachal_pradesh', 'arunachal pradesh', 'ap',
                   '12-arunachal pradesh', '12', 12],
            '13': ['nagaland', 'nagaland', 'nl', '13-nagaland', '13', 13],
            '14': ['manipur', 'manipur', 'mn', '14-manipur', '14', 14],
            '15': ['mizoram', 'mizoram', 'mz', '15-mizoram', '15', 15],
            '16': ['tripura', 'tripura', 'tr', '16-tripura', '16', 16],
            '17': ['meghalaya', 'meghalaya', 'ml', '17-meghalaya', '17', 17],
            '18': ['assam', 'assam', 'as', '18-assam', '18', 18],
            '19': ['west bengal', 'west-bengal', 'west_bengal', 'west bengal', 'wb', '19-west bengal', 19, '19'],
            '20': ['jharkhand', 'jharkhand', 'jk', '20-jharkhand', '20', 20],
            '21': ['odisha', 'odisa', 'orissa', 'od', 'or', 'odisha', '21-odisha', '21', 21],
            '22': ['chhattisgarh', 'chattisgarh', 'cg', 'ct', 'chhattisgarh', '22-chhattisgarh', '22', 22],
            '23': ['madhya pradesh', 'madhya_pradesh', 'madhya-pradesh', 'mp', 'madhya pradesh', '23-madhya pradesh', '23',
                   23],
            '24': ['gujarat', 'gujarat', 'gujraat', 'gj', 'gujrat', '24-gujarat', '24', 24],
            '25': ['daman & diu', 'daman and diu', 'diu & daman', 'dd', 'diu and daman', '25-daman and diu', '25', 25],
            '26': ['dadra & nagar haveli', 'dadra & nagar haveli', 'dn', '26-dadra & nagar haveli', '26', 26],
            '27': ['maharashtra', 'maharastra', 'mh', 'maharashtra', '27-maharashtra', '27', 27],
            '29': ['karnataka', 'karnataka', 'ka', '29-karnataka', '29', 29],
            '30': ['goa', 'goa', 'ga', '30-goa', '30', 30],
            '31': ['lakshdweep', 'lakshadweep islands', 'ld', 'lakshdweep', '31-lakshdweep', '31', 31],
            '32': ['kerala', 'kerala', 'kl', '32-kerala', '32', 32],
            '33': ['tamil nadu', 'tamil-nadu', 'tamil_nadu', 'tamilnadu', 'tamil nadu', 'tn', '33-tamil nadu', '33', 33],
            '34': ['pondicherry', 'pondicherry', 'py', '34-pondicherry', '34', 34],
            '35': ['andaman & nicobar islands', 'andaman & nicobar', 'andaman and nicobar islands', 'andaman and nicobar',
                   'an',
                   '35-andaman & nicobar islands', '35', 35],
            '36': ['telengana', 'telangana', 'ts', 'telengana', '36-telengana', '36', 36],
            '37': ['andhra pradesh', 'andhra_pradesh', 'andhra-pradesh', 'andhrapradesh', 'ad', 'ap', '37-andhra pradesh',
                   37,
                   28, '37', '28'],
            '97': ['other territory', 'other-territory', 'other_territory', 'otherterritory', 'oth', '97-other territory',
                   97,
                   '97'],
            'na': ['NA', 'na', 'nan', '0']}

        def argcontains(item):
            for i, v in state_master_dict.items():
                if item in v:
                    return i

        user = 'taxgenie'
        passw = 'taxgenie*#8102*$'
        host = '15.206.93.178'
        port = 3306
        database = 'taxgenie_efilling'
        mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database,
                             echo=False)
        d = pd.read_sql("select * from sales_invoice_header where typeofinvoice in('b2ba','cnr','dnr')", mydb)

        def func(x):
            try:
                if (re.match(r'[\s\w\d]+[/.-]+[\w\d]+[./-]+[\w\d\W]+', str(x))):
                    return pd.to_datetime(x, errors='coerce')

                elif (re.match(r'(?:\d{5})[.](?:\d{1,})|(?:\d{5})', str(x))):
                    x = datetime.fromtimestamp(timestamp=(x - 25569) * 86400).strftime('%d-%m-%Y')
                    return pd.to_datetime(x, errors='coerce')

                elif (re.match(r'[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)', str(x))):
                    x = time.strftime('%d-%m-%Y', time.localtime(x))
                    return pd.to_datetime(x, errors='coerce')
            except:
                return pd.NaT

        # +++++++++++++++++++++++++++++++++++++++Common for all+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        def Common_check(df):
            df['reason'] = ''
            print('inside common check validation')

            # .....................Seller GSTIN..................................

            df.loc[(df['Seller Gstin'].astype(str).str.lower() == 'na') | (df['Seller Gstin'].isna()), 'reason'] = df[
                                                                                                                       'reason'] + " Seller GSTIN should not be blank."
            df.loc[(((df['Seller Gstin'].astype(str).str.lower() != 'na') & (df['Seller Gstin'].notna())) & (
                        df['Seller Gstin'].astype(str).str.len() != 15)), 'reason'] = df[
                                                                                          'reason'] + " Max size of Seller GSTIN No Should be 15."
            df.loc[((df['Seller Gstin'].astype(str).str.match(
                "[0-9]{2}[a-zA-Z]{5}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[Z]{1}[0-9a-zA-Z]{1}") == False) & (
                                (df['Seller Gstin'].astype(str).str.lower() != 'na') & (
                            df['Seller Gstin'].notna()))), 'reason'] = df['reason'] + " Invalid Seller GSTIN."

            # .....................Invoice Nmuber...................................
            df.loc[((df['Invoice No'] == 0) | (df['Invoice No'].astype(str).str.lower() == 'na') | (
                df['Seller Gstin'].isna())), 'reason'] = df['reason'] + " Invoice No should not be blank."
            df.loc[((df['Invoice No'].astype(str).str.len() > 16) & (
                        (df['Invoice No'] != 0) & (df['Invoice No'].astype(str).str.lower() != 'na') & (
                    df['Seller Gstin'].notna()))), 'reason'] = df['reason'] + " , Max size of Invoice No should be 16."
            df.loc[((df['Invoice No'].astype(str).str.contains(r'[^a-zA-Z0-9/-]')) & (
                        (df['Invoice No'] != 0) & (df['Invoice No'].astype(str).str.lower() != 'na') & (
                    df['Seller Gstin'].notna()))), 'reason'] = df[
                                                                   'reason'] + " Invoice No should not Contain Special Charater Except  '-'  or  '/'."

            # ....................Invoice Type........................................
            df.loc[((df['Invoice Type'].astype(str).str.lower() == 'na') | (df['Seller Gstin'].isna())), 'reason'] = df[
                                                                                                                         'reason'] + " Invoice Type should not be blank."
            df.loc[(((df['Invoice Type'].astype(str).str.lower() != 'b2b') & (
                        df['Invoice Type'].astype(str).str.lower() != 'cnr') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'cnur') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'dnr') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'dnur') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'b2cs') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'b2cl') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'exp')) & (
                                (df['Invoice Type'].astype(str).str.lower() != 'na') & (
                            df['Invoice Type'].notna()))), 'reason'] = df[
                                                                           'reason'] + " Invoice Type should be B2B,Exp,B2CS,B2CL,CNR,DRN,CNUR,DNUR."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice Type'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice Type'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice Type'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice Type must be same for same Invoice No.'

            # .................Invoice Date...........................................

            df['Invoice Date'] = df['Invoice Date'].apply(func)

            # .....................Invoice Date.....................................
            df.loc[(df['Invoice Date'].isnull()), 'reason'] = df['reason'] + " Invoice Date is Incorrect."
            df.loc[(df['Invoice Date'].astype(str).astype(str).str.lower() == 'na'), 'reason'] = df[
                                                                                                     'reason'] + " Invoice Date should not be blank."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice Date'].transform(
                                                                                 lambda x: x != x.iloc[0]) & (df[
                                                                                                                  'Invoice Date'].notnull())].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice Date must be same for same Invoice No.'
            df.loc[(pd.to_datetime('today') < df['Invoice Date']), 'reason'] = df[
                                                                                   'reason'] + " Date should not be greater then current date."
            # df.loc[((pd.datetime.now().date() - pd.DateOffset(months=18)<df['Invoice Date'])) & (df['Invoice Date'].notnull()),'reason']=df['reason']+" Invoice Date should not be 18 months older."
            df.loc[("2017-07-01" > df['Invoice Date'].astype(str)), 'reason'] = df[
                                                                                    'reason'] + " Invoice Date should be After 01-JULY-2017."

            # .....................................Rate.................................
            df.loc[((df['Rate'] == 0) | (df['Rate'].astype(str).astype(str).str.lower() == 'na') | (
                df['Rate'].isna())), 'reason'] = df['reason'] + " Please Enter Rate."
            df.loc[((df['Rate'].astype(str).astype(str).str.lower() != 'na') & (df['Rate'].notna())) & (~(
                        (df['Rate'].astype(str) == "0") | (df['Rate'].astype(str) == '0.25') | (
                            df['Rate'].astype(str) == '0.10') | (df['Rate'].astype(str) == '3') | (
                                    df['Rate'].astype(str) == '5') | (df['Rate'].astype(str) == '12') | (
                                    df['Rate'].astype(str) == '18') | (df['Rate'].astype(str) == '28'))), 'reason'] = df[
                                                                                                                          'reason'] + " Rate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."

            # .................................Invoice value............................
            df.loc[((df['Invoice Value'] == 0) | (df['Invoice Value'].astype(str).str.lower() == 'na') | (
                df['Invoice Value'].isna())), 'reason'] = df['reason'] + " Total Note Value should not be blank."
            # df.loc[~((df['Invoice Value'].astype(str).str.replace('.', '').str.isnumeric()) & ((df['Invoice Value'] != 0) | (df['Invoice Value'].astype(str).str.lower() != 'na'))), 'reason'] = df['reason'] + " Total Note Value should be numeric."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice Value'].transform(
                                                                                 lambda x: x != x.iloc[0]) & (
                                                                                         (df['Invoice Value'] != 0) | ((df[
                                                                                                                            'Invoice Type'].astype(
                                                                                     str).str.lower() != 'na') & (df[
                                                                                                                      'Invoice Type'].notna())))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Total Note Value must be same for same Note No.'

            df.loc[~((df['Rate'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                     'reason'] + " Rate Value should be numeric"
            df.loc[~((df['Invoice Value'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                              'reason'] + " Invoice Value should be numeric"

            if ('Taxable Value' in list(df.columns)):
                df.loc[(df['Taxable Value'].isna()), 'reason'] = df['reason'] + " Taxable Value should not be blank"
                df.loc[~((df['Taxable Value'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                                  'reason'] + " Taxable Value should be numeric"

            if ('Igst Rate' in list(df.columns)):
                df['Igst Rate'] = df['Igst Rate'].replace('',0).fillna(0)


                df.loc[(~(df['Igst Rate'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                              'reason'] + " Igst Rate Value should be numeric"
            if ('Igst Amount' in list(df.columns)):
                df['Igst Amount'] = df['Igst Amount'].replace('',0).fillna(0)
                df.loc[(~(df['Igst Amount'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                                'reason'] + " Igst Amount Value should be numeric"

            if ('Cgst Rate' in list(df.columns)):
                df['Cgst Rate'] = df['Cgst Rate'].replace('',0).fillna(0)
                df.loc[(~(df['Cgst Rate'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                              'reason'] + " Cgst Rate Value should be numeric"
            if ('Cgst Amount' in list(df.columns)):
                df['Cgst Amount'] = df['Cgst Amount'].replace('',0).fillna(0)
                df.loc[(~(df['Cgst Amount'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                                'reason'] + " Cgst Amount Value should be numeric"

            if ('Sgst Rate' in list(df.columns)):
                df['Sgst Rate'] = df['Sgst Rate'].replace('',0).fillna(0)
                df.loc[(~(df['Sgst Rate'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                              'reason'] + " Sgst Rate Value should be numeric"

            if ('Sgst Amount' in list(df.columns)):
                df['Sgst Amount'] = df['Sgst Amount'].replace('',0).fillna(0)
                df.loc[(~(df['Sgst Amount'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                                'reason'] + " Sgst Amount Value should be numeric"

            if ('Cess Amount' in list(df.columns)):
                df['Cess Amount'] = df['Cess Amount'].replace('', 0).fillna(0)
                df.loc[(~(df['Cess Amount'].astype(str).str.replace('.', '').str.isnumeric())), 'reason'] = df[
                                                                                                                'reason'] + " Cess Amount Value should be numeric"
            if('Place Of Supply' in list(df.columns)):
                df['Place Of Supply'] = df['Place Of Supply'].astype(str).astype(str).str.lower().map(argcontains)

            return df

        def B2B_validation(b2b):
            print('inside b2b validation')
            df = b2b
            df = df.reset_index(drop=True)
            # ....................................POS funcation.........................................................
            # .....................Amendment......................................
            df.loc[df.set_index(['Seller Gstin', 'Buyer Gstin', 'Invoice No']).index.isin(
                d.set_index(['sellerGSTIN', 'buyerGSTIN', 'oldInvoiceNo']).index), 'reason'] = df[
                                                                                                   'reason'] + ' You have already created Amendmend'
            df.loc[df.set_index(['Seller Gstin', 'Buyer Gstin', 'Invoice No']).index.isin(
                d.set_index(['sellerGSTIN', 'buyerGSTIN', 'refrInvoiceNo']).index), 'reason'] = df[
                                                                                                    'reason'] + ' You have already created Credit/Debit'

            # .........................Buyer GSTIN.....................................
            df.loc[
                ((df['Buyer Gstin'].astype(str).astype(str).str.lower() == 'na') | (df['Buyer Gstin'].isna())), 'reason'] = \
            df['reason'] + " Buyer Gstin should not be blank."
            df.loc[((df['Buyer Gstin'].astype(str).str.lower() != 'na') & (df['Buyer Gstin'].notna())) & (
                        df['Buyer Gstin'].astype(str).str.len() != 15), 'reason'] = df[
                                                                                        'reason'] + " Max size of Buyer Gstin No Should be 15."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Buyer Gstin'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice Type'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice Type'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Buyer Gstin must be same for same Invoice No.'
            df.loc[(df['Buyer Gstin'].astype(str).str.match(
                "[0-9]{2}[a-zA-Z]{5}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[Z]{1}[0-9a-zA-Z]{1}") == False) & (
                               (df['Buyer Gstin'].astype(str).str.lower() != 'na') & (
                           df['Buyer Gstin'].notna())), 'reason'] = df['reason'] + " Invalid GSTIN/UIN of Recipient."

            # #....................Invoice SubType...........................................................

            df.loc[((df['Invoice SubType'].astype(str).str.lower() == 'na') | (df['Invoice SubType'].isna())), 'reason'] = \
            df['reason'] + " Invoice SubType should not be blank."
            df.loc[(((df['Invoice SubType'].astype(str).str.lower() != 'regular') & (
                        df['Invoice SubType'].astype(str).str.lower() != 'de') & (
                                 df['Invoice SubType'].astype(str).str.lower() != 'sewp') & (
                                 df['Invoice SubType'].astype(str).str.lower() != 'cbw') & (
                                 df['Invoice SubType'].astype(str).str.lower() != 'sewop')) & (
                                (df['Invoice SubType'].astype(str).str.lower() != 'na') & (
                            df['Invoice SubType'].notna()))), 'reason'] = df[
                                                                              'reason'] + " InvoiceSubType should be REGULAR,DE,SEWP,SEWOP,Sale from Bonded WH."
            # df.loc[pd.DataFrame(df['Invoice SubType'].tolist()).isin(list(df['Invoice SubType'].loc[df.groupby('Invoice SubType')['Invoice No'].transform(lambda x: x!= x.iloc[0])&(df['Invoice SubType'].astype(str).str.lower()!='na')].unique())).any(1),'reason']=df['reason']+',Invoice Date must be same for same Invoice No'
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice SubType'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice Type'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice Type'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice SubType must be same for same Invoice No.'

            # #....................RCM......................................................................

            df.loc[(df['Reverse Charge'].astype(str).str.lower() == 'na') | (df['Reverse Charge'].isna()), 'reason'] = df[
                                                                                                                           'reason'] + " Reverse Charge should not be blank."
            df.loc[(df['Reverse Charge'].astype(str).str.lower() != 'y') & (
                        df['Reverse Charge'].astype(str).str.lower() != 'n') & (
                               (df['Reverse Charge'].astype(str).str.lower() != 'na') & (
                           df['Reverse Charge'].notna())), 'reason'] = df[
                                                                           'reason'] + " Reverse Charge should  be Y/N blank."
            # df.loc[pd.DataFrame(df['Reverse Charge'].tolist()).isin(list(df['Reverse Charge'].loc[df.groupby('Reverse Charge')['Invoice No'].transform(lambda x: x!= x.iloc[0])&(df['Reverse Charge'].astype(str).str.lower()!='na')].unique())).any(1),'reason']=df['reason']+',Reverse Charge must be same for same Invoice No'
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Reverse Charge'].transform(
                                                                                 lambda x: x != x.iloc[0]) & (((df[
                                                                                                                    'Invoice Type'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice Type'].notna())))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Reverse Charge must be same for same Invoice No.'

            # #.................................Taxable Value...................................................

            df['Taxable Value'] = df['Taxable Value'].apply(pd.to_numeric, lambda x: '%.2f' % x, errors='ignore')
            df.loc[(df['Taxable Value'] == 0) | (df['Taxable Value'].isna()), 'reason'] = df[
                                                                                              'reason'] + " Taxable Value should not be blank."
            df.loc[~(df['Taxable Value'].astype(str).str.replace('.', '').str.isnumeric()), 'reason'] = df[
                                                                                                            'reason'] + " Taxable Value should be numeric."

            # ............................pos validation.........................................................

            df.loc[(df['Place Of Supply'].astype(str).str.lower() == 'na'), 'reason'] = df[
                                                                                            'reason'] + " Place Of Supply should not be blank."
            df.loc[(df['Place Of Supply'].isna()), 'reason'] = df['reason'] + " Invalid Place Of Supply"
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice Type'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice Type'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # ....................................TDS Validations ............................
            df.loc[((df['Buyer Gstin'].astype(str).str.match(
                "[0-9]{2}[a-zA-Z]{4}[a-zA-Z0-9]{1}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[D]{1}[0-9a-zA-Z]{1}") == True) & (
                                df['Buyer Gstin'].astype(str).str.match(
                                    "[0-9]{2}[a-zA-Z]{4}[0-9]{1}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[D]{1}[0-9a-zA-Z]{1}") == True)) & (
                       df['Buyer Gstin'].notna()), 'reason'] = df[
                                                                   'reason'] + " Receiver is of TDS type, so you cannot upload."
            df.loc[((df['Buyer Gstin'] == df['Seller Gstin']) & (df['Seller Gstin'].notna())), 'reason'] = df[
                                                                                                               'reason'] + " GSTIN/UIN of Recipient And Seller GSTIN should not be same."

            # #...................................Unit........................................................
            if ('unit' in list(df.columns)):
                unitofmeasurement = ['BAG', 'BGS', 'BKL', 'BOU', 'BOX', 'BTL', 'BUN', 'CBM', 'CCM', 'CIN', 'CMS', 'CQM',
                                     'CTN',
                                     'DOZ', 'DRM', 'FTS', 'GGR', 'GMS', 'GRS', 'GYD', 'HKS', 'INC', 'KGS', 'KLR', 'KME',
                                     'LBS',
                                     'LOT', 'LTR', 'MGS', 'MTR', 'MTS', 'NOS', 'ODD', 'PAC', 'PCS', 'PRS', 'QTL', 'ROL',
                                     'SDM',
                                     'SET', 'SHT', 'SQF', 'SQI', 'SQM', 'SQY', 'TBS', 'THD', 'TOL', 'TON', 'TUB', 'UGS',
                                     'UNT',
                                     'VLS', 'YDS']
                df.loc[df['unit'].isin(list(filter(lambda x: x not in unitofmeasurement, list(df['unit'])))), 'reason'] = \
                df['reason'] + ' Invalid Unit. It Should be as Per Gov. Standard.'

                # #...................................Quantity................................................ .

            if ('quantity' in list(df.columns)):
                df.loc[~(df['quantity'].astype(str).str.replace('.', '').str.isnumeric()), 'reason'] = df[
                                                                                                           'reason'] + " Quantity should be Numeric."

                # #...................................Description.............................

            if ('Description' in list(df.columns)):
                df.loc[(df['Description'].astype(str).str.len() >= 40), 'reason'] = df[
                                                                                        'reason'] + " Max size of Description  Should be 40."

            # #...................................HSN/SAC................................

            if ('Hsn Code' in list(df.columns)):
                df.loc[~(df['Hsn Code'].astype(str).apply(lambda x: 2 <= len(x) <= 8)) & (
                (df['Hsn Code'].astype(str).str.isnumeric())), 'reason'] = df[
                                                                               'reason'] + " HSNOrSAC length should be between 2 to 8."
                df.loc[~(df['Hsn Code'].astype(str).str.isnumeric()), 'reason'] = df[
                                                                                      'reason'] + " HSNOrSAC should be Numeric."

            # #.....................Ecomm GSTIN...........................................

            if ('E Commerce GSTIN' in list(df.columns)):
                df.loc[(df['E Commerce GSTIN'].astype(str).str.match(
                    "[0-9]{2}[a-zA-Z]{5}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[Z]{1}[0-9a-zA-Z]{1}") == False) & (
                                   (df['E Commerce GSTIN'].astype(str).str.lower() != 'na') & (
                                       df['E Commerce GSTIN'] != 0) & (df['E Commerce GSTIN'].notna())), 'reason'] = df[
                                                                                                                         'reason'] + " Invalid GSTIN/UIN of Recipient."
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'E Commerce GSTIN'].transform(
                                                                                     lambda x: x != x.iloc[0]) & ((df[
                                                                                                                       'Invoice Type'].astype(
                                                                                     str).str.lower() != 'na') & (df[
                                                                                                                      'Invoice Type'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Ecomm GSTIN must be same for same Invoice No.'

            # .....................Buyer Company Name......................................

            if ('Buyer Company Name' in list(df.columns)):

                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'Buyer Company Name'].transform(
                                                                                     lambda x: x != x.iloc[0]) & ((df[
                                                                                                                       'Invoice Type'].astype(
                                                                                     str).str.lower() != 'na') & (df[
                                                                                                                      'Invoice Type'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Buyer Company Name must be same for same invoiceNo.'

            if ((('Sgst Rate' in list(df.columns)) and ('Cgst Rate' in list(df.columns))) or (
                    'Igst Rate' in list(df.columns))):

                if ((('Sgst Rate' in list(df.columns)) and ('Cgst Rate' in list(df.columns)))):
                    df.loc[~(((df['Cgst Rate'] + df['Sgst Rate']) == 0) | ((df['Cgst Rate'] + df['Sgst Rate']) == 0.25) | (
                                (df['Cgst Rate'] + df['Sgst Rate']) == 0.10) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 3) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 5) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 12) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 18) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 28)), 'reason'] = df[
                                                                                                      'reason'] + " Addition of CgstRate & SgstOrUgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."
                    df.loc[(((df['Seller Gstin'].str[:2] != df['Place Of Supply']) & (
                                df['Place Of Supply'].astype(str).str.lower() != "na")) & (
                                        df['Cgst Rate'] != df['Sgst Rate'])), 'reason'] = df[
                                                                                              'reason'] + " CGSTRate & SGSTOrUgstRate should be Equal For Intra State."
                    df.loc[(((df['Invoice SubType'].astype(str).str.lower() == "de") | (
                                df['Invoice SubType'].astype(str).str.lower() == "sewp")) & (
                                        (df['Cgst Rate'] != 0) & (df['Sgst Rate'] != 0))), 'reason'] = df[
                                                                                                           'reason'] + " SgstOrUgstRate or CgstRate Should be '0' For subtype."
                    df.loc[((df['Invoice SubType'].astype(str).str.lower() == "sewop") & (
                                (df['Cgst Rate'] != 0) | (df['Sgst Rate'] != 0))), 'reason'] = df[
                                                                                                   'reason'] + " SgstOrUgstRate or CgstRate Should be '0' For subtype."

                if (('Igst Rate' in list(df.columns))):
                    df.loc[~(((df['Invoice SubType'].astype(str).str.lower() != "de") | (
                                df['Invoice SubType'].astype(str).str.lower() != "sewp")) & (
                                         (df['Igst Rate'] == 0) | (df['Igst Rate'] == 0.25) | (df['Igst Rate'] == 0.10) | (
                                             df['Igst Rate'] == 3) | (df['Igst Rate'] == 5) | (df['Igst Rate'] == 12) | (
                                                     df['Igst Rate'] == 18) | (df['Igst Rate'] == 28))), 'reason'] = df[
                                                                                                                         'reason'] + " IgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28"
                    df.loc[~((df['Igst Rate'] == 0) | (df['Igst Rate'] == 0.25) | (df['Igst Rate'] == 0.10) | (
                                df['Igst Rate'] == 3) | (df['Igst Rate'] == 5) | (df['Igst Rate'] == 12) | (
                                         df['Igst Rate'] == 18) | (df['Igst Rate'] == 28)), 'reason'] = df[
                                                                                                            'reason'] + " IgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28 "
                    df.loc[(((df['Seller Gstin'].str[:2] == df['Place Of Supply']) & (
                                df['Place Of Supply'].astype(str).str.lower() != "na") & (df['Place Of Supply'].notna()) & (
                                         df['Igst Rate'] != 0))), 'reason'] = df[
                                                                                  'reason'] + " IGST Rate should be '0' For Intra State."
                    df.loc[~((df['Invoice SubType'].astype(str).str.lower() != "sewop") & (
                                (df['Igst Rate'] == 0) | (df['Igst Rate'] == 0.25) | (df['Igst Rate'] == 0.10) | (
                                    df['Igst Rate'] == 3) | (df['Igst Rate'] == 5) | (df['Igst Rate'] == 12) | (
                                            df['Igst Rate'] == 18) | (df['Igst Rate'] == 28))), 'reason'] = df[
                                                                                                                'reason'] + " IgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28"

            if (('Sgst Amount' in list(df.columns)) and ('Cgst Amount' in list(df.columns)) or (
                    'Igst Amount' in list(df.columns))):

                if (('Sgst Amount' in list(df.columns)) and ('Cgst Amount' in list(df.columns))):
                    df.loc[(((df['Seller Gstin'].str[:2] != df['Place Of Supply']) & (
                                df['Place Of Supply'].astype(str).str.lower() != "na")) & (
                                        (df['Cgst Amount'] != 0) | (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                               'reason'] + " SGSTOrUgstAmt or CGSTAmt Should be '0' For Inter State."
                    df.loc[((df['Invoice SubType'].astype(str).str.lower() == "sewop") & (
                                (df['Cgst Amount'] != 0) | (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                       'reason'] + " SgstOrUgstAmount or CgstAmount Should be '0' For subtype."
                    df.loc[(((df['Invoice SubType'].astype(str).str.lower() == "de") | (
                                df['Invoice SubType'].astype(str).str.lower() == "sewp")) & (
                                        (df['Cgst Amount'] != 0) | (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                               'reason'] + " SgstOrUgstAmount or CgstAmount Should be '0' For subtype."

                if (('Igst Amount' in list(df.columns))):
                    df.loc[(((df['Seller Gstin'].str[:2] == df['Place Of Supply']) & (
                                df['Place Of Supply'].astype(str).str.lower() != "na") & (df['Place Of Supply'].notna()) & (
                                         df['Igst Amount'] != 0))), 'reason'] = df[
                                                                                    'reason'] + " Igst Amount should be '0' For Intra State."
                    df.loc[
                        ((df['Invoice SubType'].astype(str).str.lower() == "sewop") & (df['Igst Amount'] != 0)), 'reason'] = \
                    df['reason'] + " Igst Amount should be '0' For subtype."
                    df.loc[((df['Invoice SubType'].astype(str).str.lower() == "sewop") & (((df['Igst Rate'] != 0) & (
                                df['Igst Amount'].astype(float) != (df['Igst Rate'] * (df['Taxable Value'] / 100)).astype(
                            float))) & ((df['Igst Rate'] != 0) & (round(df['Igst Amount'].astype(float)) != (
                        round(df['Igst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                        df['Igst Amount'].astype(float)) - (round((df['Igst Rate'].astype(float)) * (
                                (df['Taxable Value'].astype(float)) / 100)))).abs() != 0))), 'reason'] = df[
                                                                                                             'reason'] + " Incorrect IGST Amount."

                if (('Igst Rate' in list(df.columns)) and ('Igst Amount' in list(df.columns))):
                    df.loc[(((df['Igst Rate'] != 0) & (
                                df['Igst Amount'].astype(float) != (df['Igst Rate'] * (df['Taxable Value'] / 100)).astype(
                            float))) & ((df['Igst Rate'] != 0) & (round(df['Igst Amount'].astype(float)) != (
                        round(df['Igst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                        df['Igst Amount'].astype(float)) - (round((df['Igst Rate'].astype(float)) * (
                                (df['Taxable Value'].astype(float)) / 100)))).abs() > 0)), 'reason'] = df[
                                                                                                           'reason'] + " Incorrect IGST Amount."

                if (('Sgst Rate' in list(df.columns)) and ('Sgst Amount' in list(df.columns))):
                    df.loc[(((df['Sgst Rate'] != 0) & (
                                df['Sgst Amount'].astype(float) != (df['Sgst Rate'] * (df['Taxable Value'] / 100)).astype(
                            float))) & ((df['Sgst Rate'] != 0) & (round(df['Sgst Amount'].astype(float)) != (
                        round(df['Sgst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                        df['Sgst Amount'].astype(float)) - (round((df['Sgst Rate'].astype(float)) * (
                                (df['Taxable Value'].astype(float)) / 100)))).abs() > 5)), 'reason'] = df[
                                                                                                           'reason'] + " Incorrect SGST Amount."

                if (('Cgst Rate' in list(df.columns)) and ('Cgst Amount' in list(df.columns))):
                    df.loc[(((df['Cgst Rate'] != 0) & (
                                df['Cgst Amount'].astype(float) != (df['Cgst Rate'] * (df['Taxable Value'] / 100)).astype(
                            float))) & ((df['Cgst Rate'] != 0) & (round(df['Cgst Amount'].astype(float)) != (
                        round(df['Cgst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                        df['Cgst Amount'].astype(float)) - (round((df['Cgst Rate'].astype(float)) * (
                                (df['Taxable Value'].astype(float)) / 100)))).abs() > 5)), 'reason'] = df[
                                                                                                           'reason'] + " Incorrect CGST Amount."

            # Final data conversion in dd-mm-yy format........
            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        def B2C_validation(b2c):
            df = b2c
            df = df.reset_index(drop=True)

            df['Place Of Supply'] = df['Place Of Supply'].astype(str).astype(str).str.lower().map(argcontains)

            # .....................Amendment & Credit Note......................................
            df.loc[df.set_index(['Seller Gstin', 'Buyer Gstin', 'Invoice No']).index.isin(
                d.set_index(['sellerGSTIN', 'buyerGSTIN', 'oldInvoiceNo']).index), 'reason'] = df[
                                                                                                   'reason'] + ' You have already created Amendmend'
            df.loc[df.set_index(['Seller Gstin', 'Buyer Gstin', 'Invoice No']).index.isin(
                d.set_index(['sellerGSTIN', 'buyerGSTIN', 'refrInvoiceNo']).index), 'reason'] = df[
                                                                                                    'reason'] + ' You have already created Credit/Debit'

            # #.................................Taxable Value............................
            df['Taxable Value'] = df['Taxable Value'].apply(pd.to_numeric, lambda x: '%.2f' % x, errors='ignore')
            df.loc[(df['Taxable Value'] == 0) | (df['Taxable Value'].isna()), 'reason'] = df[
                                                                                              'reason'] + " Taxable Value should not be blank."
            df.loc[~(df['Taxable Value'].astype(str).str.replace('.', '').str.isnumeric()), 'reason'] = df[
                                                                                                            'reason'] + " Taxable Value should be numeric."

            # ............................pos validation.........................................
            df.loc[(df['Place Of Supply'].astype(str).str.lower() == 'na'), 'reason'] = df[
                                                                                            'reason'] + " Place Of Supply should not be blank."
            df.loc[(df['Place Of Supply'].isnull()), 'reason'] = df['reason'] + " Invalid Place Of Supply"
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & (
                                                                                         df['Invoice No'].astype(
                                                                                             str).str.lower() != 'na')].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # ...................................INTER-STATE for B2CL type.................................

            df.loc[((df['Invoice Type'].astype(str).str.lower() == 'b2cl') & (
                        df['Seller Gstin'].str[:2] == df['Place Of Supply'])), 'reason'] = df[
                                                                                               'reason'] + " POS should be INTER-STATE for B2CL type"

            # ...................................greater than 2.5lac.................................

            df.loc[((df['Invoice Type'].astype(str).str.lower() == 'b2cl') & (df['Invoice Value'] < 250000)), 'reason'] = \
            df['reason'] + " Invoice Value should be greater than 2.5lac for B2CL"
            df.loc[((df['Invoice Type'].astype(str).str.lower() == 'b2cs') & (df['Invoice Value'] > 250000)), 'reason'] = \
            df['reason'] + " Invoice Value should be less than 2.5lac for B2CS"

            # ...........................Only for B2CS RCM.................................................
            ls = list(df['Invoice Type'].unique())

            if ("B2CS" in ls):
                # ..............................RCM........................................................
                df.loc[(df['Reverse Charge'].astype(str).str.lower() == 'na') | (df['Reverse Charge'].isna()), 'reason'] = \
                df['reason'] + " Reverse Charge should not be blank."
                df.loc[(df['Reverse Charge'].astype(str).str.lower() != 'y') & (
                            df['Reverse Charge'].astype(str).str.lower() != 'n') & (
                                   (df['Reverse Charge'].astype(str).str.lower() != 'na') & (
                               df['Reverse Charge'].notna())), 'reason'] = df[
                                                                               'reason'] + " Reverse Charge should  be Y/N blank."
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'Reverse Charge'].transform(
                                                                                     lambda x: x != x.iloc[0]) & (((df[
                                                                                                                        'Invoice Type'].astype(
                                                                                     str).str.lower() != 'na') & (df[
                                                                                                                      'Invoice Type'].notna())))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Reverse Charge must be same for same Invoice No.'

            # +++++++++++++++++++++++++++++++++++++++++++++++++++Optinal validation+++++++++++++++++++++++++++++++++++++++++++++++++++

            # #...................................Unit.................................
            if ('unit' in list(df.columns)):
                unitofmeasurement = ['BAG', 'BGS', 'BKL', 'BOU', 'BOX', 'BTL', 'BUN', 'CBM', 'CCM', 'CIN', 'CMS', 'CQM',
                                     'CTN',
                                     'DOZ', 'DRM', 'FTS', 'GGR', 'GMS', 'GRS', 'GYD', 'HKS', 'INC', 'KGS', 'KLR', 'KME',
                                     'LBS',
                                     'LOT', 'LTR', 'MGS', 'MTR', 'MTS', 'NOS', 'ODD', 'PAC', 'PCS', 'PRS', 'QTL', 'ROL',
                                     'SDM',
                                     'SET', 'SHT', 'SQF', 'SQI', 'SQM', 'SQY', 'TBS', 'THD', 'TOL', 'TON', 'TUB', 'UGS',
                                     'UNT',
                                     'VLS', 'YDS']
                df.loc[df['unit'].isin(list(filter(lambda x: x not in unitofmeasurement, list(df['unit'])))), 'reason'] = \
                df['reason'] + ' Invalid Unit. It Should be as Per Gov. Standard.'

                # #...................................Quantity.............................

            if ('quantity' in list(df.columns)):
                df.loc[~(df['quantity'].astype(str).str.replace('.', '').str.isnumeric()), 'reason'] = df[
                                                                                                           'reason'] + " Quantity should be Numeric."

                # #...................................Description.............................

            if ('Description' in list(df.columns)):
                df.loc[(df['Description'].astype(str).str.len() >= 40), 'reason'] = df[
                                                                                        'reason'] + " Max size of Description  Should be 40."

            # #...................................HSN/SAC................................

            if ('Hsn Code' in list(df.columns)):
                df.loc[~(df['Hsn Code'].astype(str).apply(lambda x: 2 <= len(x) <= 8)) & (
                (df['Hsn Code'].astype(str).str.isnumeric())), 'reason'] = df[
                                                                               'reason'] + " HSNOrSAC length should be between 2 to 8."
                df.loc[~(df['Hsn Code'].astype(str).str.isnumeric()), 'reason'] = df[
                                                                                      'reason'] + " HSNOrSAC should be Numeric."

            # #.....................Ecomm GSTIN...........................................

            if ('E Commerce GSTIN' in list(df.columns)):
                df.loc[((df['E Commerce GSTIN'].astype(str).str.lower() != 'na') & (df['E Commerce GSTIN'].notna()) & (
                            df['E Commerce GSTIN'].astype(str).str.len() != 15)), 'reason'] = df[
                                                                                                  'reason'] + " Max Size of Ecomm GSTIN Should be 15."
                df.loc[(df['E Commerce GSTIN'].astype(str).str.match(
                    "[0-9]{2}[a-zA-Z]{5}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[Z]{1}[0-9a-zA-Z]{1}") == False) & (
                                   (df['E Commerce GSTIN'].astype(str).str.lower() != 'na') & (
                                       df['E Commerce GSTIN'] != 0) & (df['E Commerce GSTIN'].notna())), 'reason'] = df[
                                                                                                                         'reason'] + " Invalid GSTIN/UIN of Ecomm GSTIN."
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'E Commerce GSTIN'].transform(
                                                                                     lambda x: x != x.iloc[0]) & ((df[
                                                                                                                       'Invoice Type'].astype(
                                                                                     str).str.lower() != 'na') & (df[
                                                                                                                      'Invoice Type'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Ecomm GSTIN must be same for same Invoice No.'

            # .....................Buyer Company Name......................................

            if ('Buyer Company Name' in list(df.columns)):

                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'Buyer Company Name'].transform(
                                                                                     lambda x: x != x.iloc[0]) & ((df[
                                                                                                                       'Invoice Type'].astype(
                                                                                     str).str.lower() != 'na') & (df[
                                                                                                                      'Invoice Type'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Buyer Company Name must be same for same invoiceNo.'

            # ..................Tax Rate and Tax Amount.............................................................

            if ((('Sgst Rate' in list(df.columns)) and ('Cgst Rate' in list(df.columns))) or (
                    'Igst Rate' in list(df.columns))):

                if ((('Sgst Rate' in list(df.columns)) and ('Cgst Rate' in list(df.columns)))):
                    df.loc[~(((df['Cgst Rate'] + df['Sgst Rate']) == 0) | ((df['Cgst Rate'] + df['Sgst Rate']) == 0.25) | (
                                (df['Cgst Rate'] + df['Sgst Rate']) == 0.10) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 3) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 5) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 12) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 18) | (
                                         (df['Cgst Rate'] + df['Sgst Rate']) == 28)), 'reason'] = df[
                                                                                                      'reason'] + " Addition of CgstRate & SgstOrUgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."
                    df.loc[(((df['Seller Gstin'].str[:2] != df['Place Of Supply']) & (
                                df['Place Of Supply'].astype(str).str.lower() != "na")) & (
                                        df['Cgst Rate'] != df['Sgst Rate'])), 'reason'] = df[
                                                                                              'reason'] + " CGSTRate & SGSTOrUgstRate should be Equal For Intra State."
                    df.loc[(((df['Invoice SubType'].astype(str).str.lower() == "de") | (
                                df['Invoice SubType'].astype(str).str.lower() == "sewp")) & (
                                        (df['Cgst Rate'] != 0) | (df['Sgst Rate'] != 0))), 'reason'] = df[
                                                                                                           'reason'] + " SgstOrUgstRate or CgstRate Should be '0' For subtype."
                    df.loc[((df['Invoice SubType'].astype(str).str.lower() == "sewop") & (
                                (df['Cgst Rate'] != 0) | (df['Sgst Rate'] != 0))), 'reason'] = df[
                                                                                                   'reason'] + " SgstOrUgstRate or CgstRate Should be '0' For subtype."
                    df.loc[(((df['Seller Gstin'].str[:2] != df['Place Of Supply']) & (
                                df['Invoice SubType'].astype(str).str.lower() != "na")) & (
                                        (df['Cgst Rate'] != 0) & (df['Sgst Rate'] != 0))), 'reason'] = df['reason'] + " SgstOrUgstRate or CgstRate Should be '0' For Inter State."

                if (('Igst Rate' in list(df.columns))):
                    df.loc[~((df['Igst Rate'] == 0) | (df['Igst Rate'] == 0.25) | (df['Igst Rate'] == 0.10) | (
                                df['Igst Rate'] == 3) | (df['Igst Rate'] == 5) | (df['Igst Rate'] == 12) | (
                                         df['Igst Rate'] == 18) | (df['Igst Rate'] == 28)), 'reason'] = df[
                                                                                                            'reason'] + " IgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28 "
                    df.loc[(((df['Seller Gstin'].str[:2] == df['Place Of Supply']) & (
                                df['Place Of Supply'].astype(str).str.lower() != "na")) & (
                                        df['Igst Rate'] != 0)), 'reason'] = df[
                                                                                'reason'] + " IGST Rate should be '0' For Intra State."
                    df.loc[~((df['Invoice SubType'].astype(str).str.lower() != "sewop") & (
                                (df['Igst Rate'] == 0) | (df['Igst Rate'] == 0.25) | (df['Igst Rate'] == 0.10) | (
                                    df['Igst Rate'] == 3) | (df['Igst Rate'] == 5) | (df['Igst Rate'] == 12) | (
                                            df['Igst Rate'] == 18) | (df['Igst Rate'] == 28))), 'reason'] = df[
                                                                                                                'reason'] + " IgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28"

            if (('Sgst Amount' in list(df.columns)) and ('Cgst Amount' in list(df.columns)) or (
                    'Igst Amount' in list(df.columns))):

                if (('Sgst Amount' in list(df.columns)) and ('Cgst Amount' in list(df.columns))):
                    df.loc[(((df['Seller Gstin'].str[:2] != df['Place Of Supply']) & (
                                df['Place Of Supply'].astype(str).str.lower() != "na")) & (
                                        (df['Cgst Amount'] != 0) | (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                               'reason'] + " SGSTOrUgstAmt or CGSTAmt Should be '0' For Inter State."
                    df.loc[((df['Invoice SubType'].astype(str).str.lower() == "sewop") & (
                                (df['Cgst Amount'] != 0) | (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                       'reason'] + " SgstOrUgstAmount or CgstAmount Should be '0' For subtype."
                    df.loc[(((df['Invoice SubType'].astype(str).str.lower() == "de") | (
                                df['Invoice SubType'].astype(str).str.lower() == "sewp")) & (
                                        (df['Cgst Amount'] != 0) | (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                               'reason'] + " SgstOrUgstAmount or CgstAmount Should be '0' For subtype."
                    df.loc[(((df['Seller Gstin'].str[:2] != df['Invoice SubType']) & (
                                df['Invoice SubType'].astype(str).str.lower() != "na")) & (
                                        (df['Cgst Amount'] != 0) | (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                               'reason'] + " SgstOrUgstAmount or CgstAmount Should be '0' For Inter State."

                if (('Igst Amount' in list(df.columns))):
                    df.loc[(((df['Seller Gstin'].str[:2] == df['Place Of Supply']) & (
                                df['Place Of Supply'].astype(str).str.lower() != "na")) & (
                                        df['Igst Amount'] != 0)), 'reason'] = df[
                                                                                  'reason'] + " Igst Amount should be '0' For Intra State."

            if (('Igst Rate' in list(df.columns)) and ('Igst Amount' in list(df.columns))):
                df.loc[(((df['Igst Rate'] != 0) & (
                            df['Igst Amount'].astype(int) != (df['Igst Rate'] * (df['Taxable Value'] / 100)).astype(
                        int))) & ((df['Igst Rate'] != 0) & (round(df['Igst Amount'].astype(float)) != (
                    round(df['Igst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                    df['Igst Amount'].astype(float)) - (round((df['Igst Rate'].astype(float)) * (
                            (df['Taxable Value'].astype(float)) / 100)))).abs() > 0)), 'reason'] = df[
                                                                                                       'reason'] + " Incorrect IGST Amount."

            if (('Sgst Rate' in list(df.columns)) and ('Sgst Amount' in list(df.columns))):
                df.loc[(((df['Sgst Rate'] != 0) & (
                            df['Sgst Amount'].astype(int) != (df['Sgst Rate'] * (df['Taxable Value'] / 100)).astype(
                        int))) & ((df['Sgst Rate'] != 0) & (round(df['Sgst Amount'].astype(float)) != (
                    round(df['Sgst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                    df['Sgst Amount'].astype(float)) - (round((df['Sgst Rate'].astype(float)) * (
                            (df['Taxable Value'].astype(float)) / 100)))).abs() > 5)), 'reason'] = df[
                                                                                                       'reason'] + " Incorrect SGST Amount."

            if (('Cgst Rate' in list(df.columns)) and ('Cgst Amount' in list(df.columns))):
                df.loc[(((df['Cgst Rate'] != 0) & (
                            df['Cgst Amount'].astype(int) != (df['Cgst Rate'] * (df['Taxable Value'] / 100)).astype(
                        int))) & ((df['Cgst Rate'] != 0) & (round(df['Cgst Amount'].astype(float)) != (
                    round(df['Cgst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                    df['Cgst Amount'].astype(float)) - (round((df['Cgst Rate'].astype(float)) * (
                            (df['Taxable Value'].astype(float)) / 100)))).abs() > 5)), 'reason'] = df[
                                                                                                       'reason'] + " Incorrect CGST Amount."

            # Final data conversion in dd-mm-yy format........
            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        def Export_validation(exp):
            df = exp
            df = df.reset_index(drop=True)
            df['Place Of Supply'] = df['Place Of Supply'].astype(str).astype(str).str.lower().map(argcontains)
            # .....................Amendment & Credit Note......................................
            df.loc[df.set_index(['Seller Gstin', 'Buyer Gstin', 'Invoice No']).index.isin(
                d.set_index(['sellerGSTIN', 'buyerGSTIN', 'oldInvoiceNo']).index), 'reason'] = df[
                                                                                                   'reason'] + ' You have already created Amendmend'
            df.loc[df.set_index(['Seller Gstin', 'Buyer Gstin', 'Invoice No']).index.isin(
                d.set_index(['sellerGSTIN', 'buyerGSTIN', 'refrInvoiceNo']).index), 'reason'] = df[
                                                                                                    'reason'] + 'You have already created Credit/Debit'

            # #....................Invoice SubType.....................................

            df.loc[((df['Invoice SubType'].astype(str).str.lower() == 'na') | (df['Invoice SubType'].isna())), 'reason'] = \
            df['reason'] + " Invoice SubType should not be blank."
            df.loc[(((df['Invoice SubType'].astype(str).str.lower() != 'wpay') & (
                        df['Invoice SubType'].astype(str).str.lower() != 'wopay')) & (
                                (df['Invoice SubType'].astype(str).str.lower() != 'na') & (
                            df['Invoice SubType'].notna()))), 'reason'] = df['reason'] + " InvoiceType should be WPAY,WOPAY"
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice SubType'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice SubType must be same for same Invoice No.'

            # #...................................Unit.................................
            if ('unit' in list(df.columns)):
                unitofmeasurement = ['BAG', 'BGS', 'BKL', 'BOU', 'BOX', 'BTL', 'BUN', 'CBM', 'CCM', 'CIN', 'CMS', 'CQM',
                                     'CTN',
                                     'DOZ', 'DRM', 'FTS', 'GGR', 'GMS', 'GRS', 'GYD', 'HKS', 'INC', 'KGS', 'KLR', 'KME',
                                     'LBS',
                                     'LOT', 'LTR', 'MGS', 'MTR', 'MTS', 'NOS', 'ODD', 'PAC', 'PCS', 'PRS', 'QTL', 'ROL',
                                     'SDM',
                                     'SET', 'SHT', 'SQF', 'SQI', 'SQM', 'SQY', 'TBS', 'THD', 'TOL', 'TON', 'TUB', 'UGS',
                                     'UNT',
                                     'VLS', 'YDS']
                df.loc[df['unit'].isin(list(filter(lambda x: x not in unitofmeasurement, list(df['unit'])))), 'reason'] = \
                df['reason'] + ' Invalid Unit. It Should be as Per Gov. Standard.'

                # #...................................Quantity.............................

            if ('quantity' in list(df.columns)):
                df.loc[~(df['quantity'].astype(str).str.replace('.', '').str.isnumeric()), 'reason'] = df[
                                                                                                           'reason'] + " Quantity should be Numeric."

                # #...................................Description.............................

            if ('Description' in list(df.columns)):
                df.loc[(df['Description'].astype(str).str.len() >= 40), 'reason'] = df[
                                                                                        'reason'] + " Max size of Description  Should be 40."

            # #...................................HSN/SAC................................

            if ('Hsn Code' in list(df.columns)):
                df.loc[~(df['Hsn Code'].astype(str).apply(lambda x: 2 <= len(x) <= 8)) & (
                (df['Hsn Code'].astype(str).str.isnumeric())), 'reason'] = df[
                                                                               'reason'] + " HSNOrSAC length should be between 2 to 8."
                df.loc[~(df['Hsn Code'].astype(str).str.isnumeric()), 'reason'] = df[
                                                                                      'reason'] + " HSNOrSAC should be Numeric."

            # .....................Buyer Company Name......................................

            if ('Buyer Company Name' in list(df.columns)):

                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'Buyer Company Name'].transform(
                                                                                     lambda x: x != x.iloc[0]) & ((df[
                                                                                                                       'Invoice Type'].astype(
                                                                                     str).str.lower() != 'na') & (df[
                                                                                                                      'Invoice Type'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Buyer Company Name must be same for same invoiceNo.'

            # .....................Shipping Bill Date......................................

            if ('Shipping Bill Date' in list(df.columns)):
                df['Shipping Bill Date'] = df['Shipping Bill Date'].apply(func)
                df.loc[(df['Shipping Bill Date'].astype(str).str.lower() == 'na') | (
                    df['Shipping Bill Date'].isna()), 'reason'] = df['reason'] + ", Shipping Bill Date should not be blank."
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'Shipping Bill Date'].transform(
                                                                                     lambda x: x != x.iloc[0]) & ((df[
                                                                                                                       'Invoice No'].astype(
                                                                                     str).str.lower() != "na") & (df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ', Shipping Bill Date must be same for same Invoice No.'
                df.loc[("01-07-2017" > df[
                    'Shipping Bill Date']), 'reason'] = " Shipping Bill Date should be After 01-JULY-2017"
                df.loc[~(df['Invoice Date'] < df[
                    'Shipping Bill Date']), 'reason'] = " Shipping Bill Date should be after the invoice date"

            # .....................Shipping Bill Number......................................

            if ('Shipping Bill Number' in list(df.columns)):
                df.loc[(df['Shipping Bill Number'].astype(str).str.lower() == 'na') | (
                    df['Shipping Bill Number'].isna()), 'reason'] = df['reason'] + ", Shipping Bill No should not be blank."
                df.loc[((df['Shipping Bill Number'].astype(str).str.lower() != 'na') & (
                    df['Shipping Bill Number'].notna())) & (
                                   df['Shipping Bill Number'].astype(str).str.len() > 15), 'reason'] = df[
                                                                                                           'reason'] + " , Max size of Shipping Bill No should be 15."
                df.loc[df['Shipping Bill Number'].astype(str).str.contains(r'[^a-zA-Z0-9/-]') & (
                            (df['Shipping Bill Number'].astype(str).str.lower() != 'na') & (
                        df['Shipping Bill Number'].notna())), 'reason'] = df[
                                                                              'reason'] + " Invoice No should not Contain Special Charater Except  '-'  or  '/'."
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'Shipping Bill Number'].transform(
                                                                                     lambda x: x != x.iloc[0]) & ((df[
                                                                                                                       'Invoice No'].astype(
                                                                                     str).str.lower() != "na") & (df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ', Shipping Bill No must be same for same invoice No.'

            # ..............................Port Code........................................

            if ('Port Code' in list(df.columns)):
                df.loc[df['Port Code'].astype(str).str.contains(r'[^A-Za-z0-9]') & (
                            (df['Port Code'].astype(str).str.lower() != 'na') & (df['Port Code'].notna())), 'reason'] = df[
                                                                                                                            'reason'] + ", Shipping Bill Port Code Should not contain special character."
                df.loc[((df['Port Code'].astype(str).str.lower() != 'na') & (df['Port Code'].notna())) & (
                            df['Port Code'].astype(str).str.len() > 6), 'reason'] = df[
                                                                                        'reason'] + ", Shipping Bill Port Code Should be max 6 in length."
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                                 df.groupby(df['Invoice No'].astype(str))[
                                                                                     'Port Code'].transform(
                                                                                     lambda x: x != x.iloc[0]) & ((df[
                                                                                                                       'Invoice No'].astype(
                                                                                     str).str.lower() != 'na') & (df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ', Shipping Bill Port Code must be same for same invoiceNo.'

            if (('Igst Rate' in list(df.columns))):
                df.loc[~(((df['Invoice SubType'].astype(str).str.lower() != "wpay") & (
                            (df['Igst Rate'] == 0) | (df['Igst Rate'] == 0.25) | (df['Igst Rate'] == 0.10) | (
                                df['Igst Rate'] == 3) | (df['Igst Rate'] == 5) | (df['Igst Rate'] == 12) | (
                                        df['Igst Rate'] == 18) | (df['Igst Rate'] == 28)))), 'reason'] = df[
                                                                                                             'reason'] + " IgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."
                df.loc[((df['Invoice SubType'].astype(str).str.lower() == "wopay") & (
                            df['Igst Rate'] != 0)), 'reason'] = " Igst Rate should be '0' For WOPAY subtype."

            if (('Igst Amount' in list(df.columns))):

                df.loc[((df['Invoice SubType'].astype(str).str.lower() == "wopay") & (
                            df['Igst Amount'] != 0)), 'reason'] = " Igst Amount should be '0' For WOPAY subtype."

                if (('Igst Rate' in list(df.columns)) and ('Igst Amount' in list(df.columns))):
                    df.loc[(((df['Igst Rate'] != 0) & (
                                df['Igst Amount'].astype(int) != (df['Igst Rate'] * (df['Taxable Value'] / 100)).astype(
                            int))) & ((df['Igst Rate'] != 0) & (round(df['Igst Amount'].astype(float)) != (
                        round(df['Igst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                        df['Igst Amount'].astype(float)) - (round((df['Igst Rate'].astype(float)) * (
                                (df['Taxable Value'].astype(float)) / 100)))).abs() > 0)), 'reason'] = df[
                                                                                                           'reason'] + " Incorrect IGST Amount."

            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        def CDNR_validation(cdnr):
            df = cdnr
            df = df.reset_index(drop=True)
            df['Place Of Supply'] = df['Place Of Supply'].astype(str).astype(str).str.lower().map(argcontains)
            # ....................................POS funcation.........................................................
            # .........................Buyer GSTIN.....................................
            df.loc[(df['Buyer Gstin'].astype(str).str.lower() == 'na') | (df['Buyer Gstin'].isna()), 'reason'] = df[
                                                                                                                     'reason'] + " Buyer Gstin should not be blank."
            df.loc[((df['Buyer Gstin'].astype(str).str.lower() != 'na') & (df['Buyer Gstin'].notna())) & (
                        df['Buyer Gstin'].astype(str).str.len() != 15), 'reason'] = df[
                                                                                        'reason'] + " Max size of Buyer Gstin No Should be 15."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Buyer Gstin'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice Type'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice Type'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Buyer Gstin must be same for same Invoice No.'
            df.loc[(df['Buyer Gstin'].astype(str).str.match(
                "[0-9]{2}[a-zA-Z]{5}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[Z]{1}[0-9a-zA-Z]{1}") == False) & (
                               (df['Buyer Gstin'].astype(str).str.lower() != 'na') & (
                           df['Buyer Gstin'].notna())), 'reason'] = df['reason'] + " Invalid GSTIN/UIN of Recipient."
            # .....................Pre GST...........................................

            df.loc[(df['Pre GST'].astype(str).str.lower() == 'na') | (df['Pre GST'].isna()), 'reason'] = df[
                                                                                                             'reason'] + " Pre GST should not be blank."
            df.loc[(df['Pre GST'].astype(str).str.lower() != 'y') & (df['Pre GST'].astype(str).str.lower() != 'n') & (
                        (df['Pre GST'].astype(str).str.lower() != 'na') & (df['Pre GST'].notna())), 'reason'] = df[
                                                                                                                    'reason'] + " Pre GST should  be Y/N blank."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Pre GST'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).str.lower() != "na") & (df[
                                                                                                                  'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Pre GST must be same for same Note No.'

            # ......................Reference Invoice No................................

            df.loc[(df['Reference Invoice No'].astype(str).str.lower() == 'na') | (
                df['Reference Invoice No'].isna()), 'reason'] = df['reason'] + " Invoice No should not be blank."
            df.loc[((df['Reference Invoice No'].astype(str).str.lower() != 'na') & (df['Reference Invoice No'].notna())) & (
                        df['Reference Invoice No'].astype(str).str.len() > 16), 'reason'] = df[
                                                                                                'reason'] + " Max size of Invoice No should be 16."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Reference Invoice No'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).str.lower() != "na") & (df[
                                                                                                                  'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' InvoiceNo must be same for same Note No.'
            df.loc[df['Reference Invoice No'].astype(str).str.contains(r'[^a-zA-Z0-9/-]') & (
                        (df['Reference Invoice No'].astype(str).str.lower() != 'na') & (
                    df['Reference Invoice No'].notna())), 'reason'] = df[
                                                                          'reason'] + " Invoice No should not Contain Special Charater Except  '-'  or  '/'."

            # .....................Reference Invoice Date.........................
            df['Reference Invoice Date'] = df['Reference Invoice Date'].apply(func)

            df.loc[(df['Reference Invoice Date'].isnull()), 'reason'] = df['reason'] + " Invoice Date is Incorrect."
            df.loc[(df['Reference Invoice Date'].astype(str).astype(str).str.lower() == 'na'), 'reason'] = df[
                                                                                                               'reason'] + " Invoice Date should not be blank."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Reference Invoice Date'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'].isna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice Date must be same for same Note No.'
            df.loc[(pd.to_datetime('today') < df['Reference Invoice Date']), 'reason'] = df[
                                                                                             'reason'] + " Invoice Date should not be greater then current date."
            df.loc[~(df['Invoice Date'] <= df['Reference Invoice Date']), 'reason'] == df[
                'reason'] + " Note/Refund Voucher date should be after or equal to the Invoice Date."
            df.loc[((pd.datetime.now().date() - pd.DateOffset(months=18) < df['Reference Invoice Date']) & (
                df['Reference Invoice Date'].isnull())), 'reason'] = df[
                                                                         'reason'] + " Invoice Date should not be 18 months older."
            df.loc[("2017-07-01" > df['Reference Invoice Date'].astype(str)), 'reason'] = df[
                                                                                              'reason'] + " Invoice Date should be After 01-JULY-2017."
            df.loc[~((df['Invoice Date'] < df['Reference Invoice Date']) & (
                        df['Reference Invoice No'] == df['Invoice No'])), 'reason'] == df[
                'reason'] + " Note/Refund Voucher date should be after Invoice date for same Note No. & Invoice No."

            # #.................................Taxable Value............................

            df['Taxable Value'] = df['Taxable Value'].apply(pd.to_numeric, lambda x: '%.2f' % x, errors='ignore')

            # ............................pos validation.........................................
            df.loc[(df['Place Of Supply'].astype(str).str.lower() == 'na'), 'reason'] = df[
                                                                                            'reason'] + " Place Of Supply should not be blank."
            df.loc[(df['Place Of Supply'].isnull()), 'reason'] = df['reason'] + " Invalid Place Of Supply."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice Type'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice Type'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # .....................................Rate.................................
            df.loc[(df['Rate'].isna()), 'reason'] = df['reason'] + " Please Enter Rate."
            df.loc[~((df['Rate'] == 0) | (df['Rate'] == 0.25) | (df['Rate'] == 0.10) | (df['Rate'] == 3) | (
                        df['Rate'] == 5) | (df['Rate'] == 12) | (df['Rate'] == 18) | (df['Rate'] == 28)), 'reason'] = df[
                                                                                                                          'reason'] + " Rate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."

            # #....................................TDS Validations ............................
            df.loc[((df['Buyer Gstin'].astype(str).str.match(
                "[0-9]{2}[a-zA-Z]{4}[a-zA-Z0-9]{1}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[D]{1}[0-9a-zA-Z]{1}") == True) & (
                                df['Buyer Gstin'].astype(str).str.match(
                                    "[0-9]{2}[a-zA-Z]{4}[0-9]{1}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[D]{1}[0-9a-zA-Z]{1}") == True)) & (
                       df['Buyer Gstin'].notna()), 'reason'] = df[
                                                                   'reason'] + " Receiver is of TDS type, so you cannot upload."
            df.loc[((df['Buyer Gstin'] == df['Seller Gstin']) & (df['Seller Gstin'].notna())), 'reason'] = df[
                                                                                                               'reason'] + " GSTIN/UIN of Recipient And Seller GSTIN should not be same."

            # #...................................Unit.................................
            if ('unit' in list(df.columns)):
                unitofmeasurement = ['BAG', 'BGS', 'BKL', 'BOU', 'BOX', 'BTL', 'BUN', 'CBM', 'CCM', 'CIN', 'CMS', 'CQM',
                                     'CTN',
                                     'DOZ', 'DRM', 'FTS', 'GGR', 'GMS', 'GRS', 'GYD', 'HKS', 'INC', 'KGS', 'KLR', 'KME',
                                     'LBS',
                                     'LOT', 'LTR', 'MGS', 'MTR', 'MTS', 'NOS', 'ODD', 'PAC', 'PCS', 'PRS', 'QTL', 'ROL',
                                     'SDM',
                                     'SET', 'SHT', 'SQF', 'SQI', 'SQM', 'SQY', 'TBS', 'THD', 'TOL', 'TON', 'TUB', 'UGS',
                                     'UNT',
                                     'VLS', 'YDS']
                df.loc[(df['unit'].str.upper().isin(
                    list(filter(lambda x: x not in unitofmeasurement, list(df['unit'].str.upper())))) & (
                                    df['unit'].astype(str).str.lower() != "na")), 'reason'] = df[
                                                                                                  'reason'] + ' Invalid Unit. It Should be as Per Gov. Standard.'

                # #...................................Quantity.............................

            if ('quantity' in list(df.columns)):
                df.loc[~(df['quantity'].astype(str).str.lstrip('-').str.replace('.', '').str.isnumeric()) & (
                            df['quantity'].astype(str).str.lower() != "na"), 'reason'] = df[
                                                                                             'reason'] + " Quantity should be Numeric."

                # #...................................Description.............................

            if ('Description' in list(df.columns)):
                df.loc[(df['Description'].astype(str).str.len() >= 40), 'reason'] = df[
                                                                                        'reason'] + " Max size of Description  Should be 40."

            # #...................................HSN/SAC................................

            if ('Hsn Code' in list(df.columns)):
                df.loc[~(df['Hsn Code'].astype(str).apply(lambda x: 2 <= len(x) <= 8)) & (
                (df['Hsn Code'].astype(str).str.isnumeric())), 'reason'] = df[
                                                                               'reason'] + " HSNOrSAC length should be between 2 to 8."
                df.loc[~(df['Hsn Code'].astype(str).str.isnumeric()) & (
                            df['Hsn Code'].astype(str).str.lower() != "na"), 'reason'] = df[
                                                                                             'reason'] + " HSNOrSAC should be Numeric."

            if ((('Sgst Rate' in list(df.columns)) and ('Cgst Rate' in list(df.columns))) or (
                    'Igst Rate' in list(df.columns))):

                if ((('Sgst Rate' in list(df.columns)) and ('Cgst Rate' in list(df.columns)))):
                    df.loc[((df['Invoice SubType'].astype(str).astype(str).str.lower() == "wpay") & (
                                (df['Cgst Rate'] != 0) & (df['Sgst Rate'] != 0))), 'reason'] = df[
                                                                                                   'reason'] + " SgstOrUgstRate or CgstRate Should be '0' For EXPWP subtype."
                    df.loc[((df['Invoice SubType'].astype(str).astype(str).str.lower() == "wopay") & (
                                (df['Cgst Rate'] != 0) & (df['Sgst Rate'] != 0))), 'reason'] = df[
                                                                                                   'reason'] + " SgstOrUgstRate or CgstRate Should be '0' For EXPWOP subtype."

                if (('Igst Rate' in list(df.columns))):
                    df.loc[~((df['Igst Rate'] == 0) | (df['Igst Rate'] == 0.25) | (df['Igst Rate'] == 0.10) | (
                                df['Igst Rate'] == 3) | (df['Igst Rate'] == 5) | (df['Igst Rate'] == 12) | (
                                         df['Igst Rate'] == 18) | (df['Igst Rate'] == 28)), 'reason'] = df[
                                                                                                            'reason'] + " IgstRate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."

            if (('Sgst Amount' in list(df.columns)) and ('Cgst Amount' in list(df.columns)) and (
                    'Igst Amount' in list(df.columns))):

                if (('Sgst Amount' in list(df.columns)) and ('Cgst Amount' in list(df.columns))):
                    df.loc[((df['Invoice SubType'].astype(str).astype(str).str.lower() == "wpay") & (
                                (df['Cgst Amount'] != 0) & (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                       'reason'] + " SgstOrUgstAmount or CgstAmount Should be '0' For EXPWP subtype."
                    df.loc[((df['Invoice SubType'].astype(str).astype(str).str.lower() == "wopay") & (
                                (df['Cgst Amount'] != 0) & (df['Sgst Amount'] != 0))), 'reason'] = df[
                                                                                                       'reason'] + " SgstOrUgstAmount or CgstAmount Should be '0' For EXPWOP subtype."

            if (('Igst Rate' in list(df.columns)) and ('Igst Amount' in list(df.columns))):
                df['Igst Amount'] = df['Igst Amount'].fillna(0)
                df.loc[(((df['Igst Rate'] != 0) & (
                            df['Igst Amount'].astype(int) != (df['Igst Rate'] * (df['Taxable Value'] / 100)).astype(
                        int))) & ((df['Igst Rate'] != 0) & (round(df['Igst Amount'].astype(float)) != (
                    round(df['Igst Rate'].astype(float) * (df['Taxable Value'].astype(float) / 100))))) & ((round(
                    df['Igst Amount'].astype(float)) - (round((df['Igst Rate'].astype(float)) * (
                            (df['Taxable Value'].astype(float)) / 100)))).abs() > 0)), 'reason'] = df[
                                                                                                       'reason'] + " Incorrect IGST Amount."

            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%m/%d/%Y')
            return df

        def CDNUR_validation(cdnur):
            df = cdnur
            df = df.reset_index(drop=True)

            df['Place Of Supply'] = df['Place Of Supply'].astype(str).astype(str).str.lower().map(argcontains)

            # ....................................POS funcation.........................................................
            # .....................Pre GST...........................................
            df.loc[(df['Pre GST'].astype(str).str.lower() == 'na') | (df['Pre GST'].isna()), 'reason'] = df[
                                                                                                             'reason'] + " Pre GST should not be blank."
            df.loc[(df['Pre GST'].astype(str).str.lower() != 'y') & (df['Pre GST'].astype(str).str.lower() != 'n') & (
                        (df['Pre GST'].astype(str).str.lower() != 'na') & (df['Pre GST'].notna())), 'reason'] = df[
                                                                                                                    'reason'] + " Pre GST should  be Y/N blank."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Pre GST'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).str.lower() != "na") & (df[
                                                                                                                  'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Pre GST must be same for same Note No.'

            # ......................Reference Invoice No................................

            df.loc[(df['Reference Invoice No'].astype(str).str.lower() == 'na') | (
                df['Reference Invoice No'].isna()), 'reason'] = df['reason'] + " Invoice No should not be blank."
            df.loc[((df['Reference Invoice No'].astype(str).str.lower() != 'na') & (df['Reference Invoice No'].notna())) & (
                        df['Reference Invoice No'].astype(str).str.len() > 16), 'reason'] = df[
                                                                                                'reason'] + " Max size of Invoice No should be 16."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Reference Invoice No'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).str.lower() != "na") & (df[
                                                                                                                  'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' InvoiceNo must be same for same Note No.'
            df.loc[df['Reference Invoice No'].astype(str).str.contains(r'[^a-zA-Z0-9/-]') & (
                        (df['Reference Invoice No'].astype(str).str.lower() != 'na') & (
                    df['Reference Invoice No'].notna())), 'reason'] = df[
                                                                          'reason'] + " Invoice No should not Contain Special Charater Except  '-'  or  '/'."

            # .....................Reference Invoice Date.........................
            df['Reference Invoice Date'] = df['Reference Invoice Date'].apply(func)

            df.loc[(df['Reference Invoice Date'].isnull()), 'reason'] = df['reason'] + " Invoice Date is Incorrect."
            df.loc[(df['Reference Invoice Date'].astype(str).astype(str).str.lower() == 'na'), 'reason'] = df[
                                                                                                               'reason'] + " Invoice Date should not be blank."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Reference Invoice Date'].transform(
                                                                                 lambda x: x != x.iloc[0]) & (
                                                                                         df['Invoice No'].astype(
                                                                                             str).str.lower() != 'nat')].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice Date must be same for same Note No.'
            df.loc[(pd.to_datetime('today') < df['Reference Invoice Date']), 'reason'] = df[
                                                                                             'reason'] + " Invoice Date should not be greater then current date."
            df.loc[~(df['Invoice Date'] <= df['Reference Invoice Date']), 'reason'] == df[
                'reason'] + " Note/Refund Voucher date should be after or equal to the Invoice Date."
            df.loc[((pd.datetime.now().date() - pd.DateOffset(months=18) < df['Reference Invoice Date']) & (
                df['Reference Invoice Date'].isnull())), 'reason'] = df[
                                                                         'reason'] + " Invoice Date should not be 18 months older."
            df.loc[("2017-07-01" > df['Reference Invoice Date'].astype(str)), 'reason'] = df[
                                                                                              'reason'] + " Invoice Date should be After 01-JULY-2017."
            df.loc[~((df['Invoice Date'] < df['Reference Invoice Date']) & (
                        df['Reference Invoice No'] == df['Invoice No'])), 'reason'] == df[
                'reason'] + " Note/Refund Voucher date should be after Invoice date for same Note No. & Invoice No."

            # #....................Invoice SubType.....................................
            # ------------------------------------------------------------------------------------------------------------------------
            df.loc[(df['Invoice Type'].astype(str).str.lower() == 'dnur') & (
                        df['Invoice SubType'].astype(str).str.lower() == 'b2cs'), 'Invoice Type'] = "dnb2cs"
            df.loc[(df['Invoice Type'].astype(str).str.lower() == 'dnur'), 'Invoice Type'] = "Debit Note"
            df.loc[(df['Invoice Type'].astype(str).str.lower() == 'cnur') & (
                        df['Invoice SubType'].astype(str).str.lower() == 'b2cs'), 'Invoice Type'] = "cnb2cs"
            df.loc[(df['Invoice Type'].astype(str).str.lower() == 'cnur'), 'Invoice Type'] = "Credit Note"
            # ------------------------------------------------------------------------------------------------------------------------

            df.loc[((df['Invoice SubType'].astype(str).str.lower() == 'na') | (df['Invoice SubType'].isna())), 'reason'] = \
            df['reason'] + " Invoice SubType should not be blank."
            df.loc[((df['Invoice Type'].astype(str).str.lower() == 'cnb2cs') | (
                        df['Invoice Type'].astype(str).str.lower() == 'dnb2cs')) & (
                               df['Invoice SubType'].astype(str).str.lower() != 'b2cs'), 'reason'] = df[
                                                                                                         'reason'] + " Invoice SubType should be B2CS for InvoiceType B2CS Credit or B2CS Debit."
            df.loc[(((df['Invoice Type'].astype(str).str.lower() == 'credit note') | (
                        df['Invoice Type'].astype(str).str.lower() == 'debit note')) & (
                                (df['Invoice SubType'].astype(str).str.lower() != 'expwp') | (
                                    df['Invoice SubType'].astype(str).str.lower() != 'expwop') | (
                                            df['Invoice SubType'].astype(str).str.lower() != 'b2cl'))), 'reason'] = df[
                                                                                                                        'reason'] + ", Invoice SubType should be EXPWP,EXPWOP,B2CL for InvoiceType Credit Note or Debit Note."
            df.loc[(~((df['Invoice SubType'].astype(str).str.lower() == 'expwp') | (
                        df['Invoice SubType'].astype(str).str.lower() == 'expwop') | (
                                  df['Invoice SubType'].astype(str).str.lower() == 'b2cs') | (
                                  df['Invoice SubType'].astype(str).str.lower() == 'b2cl') | (
                                  df['Invoice SubType'].astype(str).str.lower() == 'sewop')) & (
                                (df['Invoice No'].astype(str).str.lower() != 'na') & (
                            df['Invoice No'].notna()))), 'reason'] = df[
                                                                         'reason'] + " Invoice SubType should be EXPWP,EXPWOP,B2CL,B2CS."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice SubType'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Note SubType must be same for same Note No.'

            # #.................................Taxable Value............................
            df['Taxable Value'] = df['Taxable Value'].apply(pd.to_numeric, lambda x: '%.2f' % x, errors='ignore')
            df.loc[(df['Taxable Value'] == 0), 'reason'] = df['reason'] + " Taxable Value should not be blank."
            df.loc[~(df['Taxable Value'].astype(str).str.replace('.', '').str.isnumeric()), 'reason'] = df[
                                                                                                            'reason'] + " Taxable Value should be numeric."

            # ............................pos validation.........................................
            df.loc[((df['Invoice SubType'].astype(str).str.lower() == 'b2cl') & (
                        df['Place Of Supply'].astype(str).str.lower() == 'na')), 'reason'] = df[
                                                                                                 'reason'] + " Place Of Supply should not be blank for B2CL."
            df.loc[(df['Place Of Supply'].isnull()), 'reason'] = df['reason'] + " Invalid Place Of Supply."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # .....................................Rate.................................
            df.loc[(df['Rate'].isna()), 'reason'] = df['reason'] + " Please Enter Rate."
            df.loc[~((df['Rate'] == 0) | (df['Rate'] == 0.25) | (df['Rate'] == 0.10) | (df['Rate'] == 3) | (
                        df['Rate'] == 5) | (df['Rate'] == 12) | (df['Rate'] == 18) | (df['Rate'] == 28)), 'reason'] = df[
                                                                                                                          'reason'] + " Rate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."

            # #...................................Unit.................................
            if ('unit' in list(df.columns)):
                unitofmeasurement = ['BAG', 'BGS', 'BKL', 'BOU', 'BOX', 'BTL', 'BUN', 'CBM', 'CCM', 'CIN', 'CMS', 'CQM',
                                     'CTN',
                                     'DOZ', 'DRM', 'FTS', 'GGR', 'GMS', 'GRS', 'GYD', 'HKS', 'INC', 'KGS', 'KLR', 'KME',
                                     'LBS',
                                     'LOT', 'LTR', 'MGS', 'MTR', 'MTS', 'NOS', 'ODD', 'PAC', 'PCS', 'PRS', 'QTL', 'ROL',
                                     'SDM',
                                     'SET', 'SHT', 'SQF', 'SQI', 'SQM', 'SQY', 'TBS', 'THD', 'TOL', 'TON', 'TUB', 'UGS',
                                     'UNT',
                                     'VLS', 'YDS']
                df.loc[df['unit'].str.upper().isin(
                    list(filter(lambda x: x not in unitofmeasurement, list(df['unit'].str.upper())))), 'reason'] = df[
                                                                                                                       'reason'] + ' Invalid Unit. It Should be as Per Gov. Standard.'
                # ...................................Quantity.............................

            if ('quantity' in list(df.columns)):
                df.loc[~(df['quantity'].astype(str).str.replace('.', '').str.isnumeric()) & (
                            df['quantity'].astype(str).astype(str).str.lower() != "na"), 'reason'] = df[
                                                                                                         'reason'] + " Quantity should be Numeric."
                # #...................................Description.............................

            if ('Description' in list(df.columns)):
                df.loc[(df['Description'].astype(str).str.len() >= 40), 'reason'] = df[
                                                                                        'reason'] + " Max size of Description  Should be 40."

            # #...................................HSN/SAC................................

            if ('Hsn Code' in list(df.columns)):
                df.loc[~(df['Hsn Code'].astype(str).apply(lambda x: 2 <= len(x) <= 8)) & (
                (df['Hsn Code'].astype(str).str.isnumeric())), 'reason'] = df[
                                                                               'reason'] + " HSNOrSAC length should be between 2 to 8."
                df.loc[~(df['Hsn Code'].astype(str).str.isnumeric()), 'reason'] = df[
                                                                                      'reason'] + " HSNOrSAC should be Numeric."

            # Final data conversion in dd-mm-yy format........
            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        ########################################Funcation Call######################################################################
        ls = stage_tb["Seller Gstin"].unique().tolist()
        print(ls)
        df_final_s = pd.DataFrame()
        for j in ls:
            s = len(str(j))
            print(s)
            print(j, 'Buyer GSTIN')
            df_f = stage_tb.loc[stage_tb['Seller Gstin'] == j]
            df_f['reason'] = ''
            df_f = df_f.reset_index()
            df3 = pd.DataFrame()
            df21 = pd.DataFrame()
            df31 = pd.DataFrame()
            df32 = pd.DataFrame()
            if (s > 9):
                if ((j[2:-3] == pan_num)):
                    print("PAN match", j)
                    print("PAN match from front", pan_num)
                    df = Common_check(df_f)

                    fail = df.loc[df['reason'] != '']
                    Pass = df.loc[df['reason'] == '']
                    Pass['Invoice Value'] = Pass['Invoice Value'].astype(float)
                    Pass['Rate'] = Pass['Rate'].astype(float)

                    if ('Taxable Value' in list(df.columns)):
                        Pass['Taxable Value'] = Pass['Taxable Value'].astype(float)
                    if ('Igst Rate' in list(df.columns)):
                        Pass['Igst Rate'] = Pass['Igst Rate'].astype(float)
                    if ('Igst Amount' in list(df.columns)):
                        Pass['Igst Amount'] = Pass['Igst Amount'].astype(float)
                    if ('Cgst Rate' in list(df.columns)):
                        Pass['Cgst Rate'] = Pass['Cgst Rate'].astype(float)
                    if ('Cgst Amount' in list(df.columns)):
                        Pass['Cgst Amount'] = Pass['Cgst Amount'].astype(float)
                    if ('Sgst Rate' in list(df.columns)):
                        Pass['Sgst Rate'] = Pass['Sgst Rate'].astype(float)
                    if ('Sgst Amount' in list(df.columns)):
                        Pass['Sgst Amount'] = Pass['Sgst Amount'].astype(float)
                    if ('Cess Amount' in list(df.columns)):
                        Pass['Cess Amount'] = Pass['Cess Amount'].astype(float)

                    ls12 = list(Pass['Invoice Type'].astype(str).unique())
                    print(ls12, 'loop based on invoice type')
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
                        df21 = df21.append(df2).reset_index(drop=True)
                    df3 = df3.append([df21, fail]).reset_index(drop=True)

                else:
                    if (j[2:-3] != pan_num):
                        print(" I am in else condtion", j)
                        print("PAN from Angular", pan_num)
                        df_f['reason'] = df_f['reason'] + 'Invalid PAN number for GSTIN'
                        df31 = df_f
                    else:
                        df_f['reason'] = df_f['reason'] + 'Invalid Buyer GSTIN'
                        df32 = df_f

            df_final_s = df_final_s.append([df32, df3, df31]).reset_index(drop=True)

        df_final_s['Status'] = np.where((df_final_s['reason'] == ''), 'Success', 'Fail')

        return df_final_s
