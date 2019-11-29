import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
from datetime import datetime
import re
from .views import *
class TG_validation_pr:

    def purchase(self,stage_tb, pan_num):
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

        # ++++++++++++++++++++++++++++++++++++++Common for all invoices+++++++++++++++++++++++++++++++++++++++++++
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
            '23': ['madhya pradesh', 'madhya_pradesh', 'madhya-pradesh', 'mp', 'madhya pradesh', '23-madhya pradesh',
                   '23', 23],
            '24': ['gujarat', 'gujarat', 'gujraat', 'gj', 'gujrat', '24-gujarat', '24', 24],
            '25': ['daman & diu', 'daman and diu', 'diu & daman', 'dd', 'diu and daman', '25-daman and diu', '25', 25],
            '26': ['dadra & nagar haveli', 'dadra & nagar haveli', 'dn', '26-dadra & nagar haveli', '26', 26],
            '27': ['maharashtra', 'maharastra', 'mh', 'maharashtra', '27-maharashtra', '27', 27],
            '29': ['karnataka', 'karnataka', 'ka', '29-karnataka', '29', 29],
            '30': ['goa', 'goa', 'ga', '30-goa', '30', 30],
            '31': ['lakshdweep', 'lakshadweep islands', 'ld', 'lakshdweep', '31-lakshdweep', '31', 31],
            '32': ['kerala', 'kerala', 'kl', '32-kerala', '32', 32],
            '33': ['tamil nadu', 'tamil-nadu', 'tamil_nadu', 'tamilnadu', 'tamil nadu', 'tn', '33-tamil nadu', '33',
                   33],
            '34': ['pondicherry', 'pondicherry', 'py', '34-pondicherry', '34', 34],
            '35': ['andaman & nicobar islands', 'andaman & nicobar', 'andaman and nicobar islands',
                   'andaman and nicobar', 'an',
                   '35-andaman & nicobar islands', '35', 35],
            '36': ['telengana', 'telangana', 'ts', 'telengana', '36-telengana', '36', 36],
            '37': ['andhra pradesh', 'andhra_pradesh', 'andhra-pradesh', 'andhrapradesh', 'ad', 'ap',
                   '37-andhra pradesh', 37,
                   28, '37', '28'],
            '97': ['other territory', 'other-territory', 'other_territory', 'otherterritory', 'oth',
                   '97-other territory', 97,
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

        def Common_check_purchase(df):
            df = df.reset_index()
            print('COMMON CHECK PURCHASE')
            # df['reason'] = ''
            # .....................Place Of Supply...................................
            df['Place Of Supply'] = df['Place Of Supply'].astype(str).astype(str).str.lower().map(argcontains)

            # .........................Buyer GSTIN.....................................
            df.loc[(((df['Buyer Gstin'].astype(str).str.match(
                "[0-9]{2}[a-zA-Z]{5}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[Z]{1}[0-9a-zA-Z]{1}") == False) & (
                                 df['Buyer Gstin'].astype(str).str.len() != 15)) & (
                                (df['Buyer Gstin'].astype(str).str.lower() != 'na') & (
                                    df['Buyer Gstin'].astype(str) != '0') & (df['Buyer Gstin'].notna()))), 'reason'] = \
            df['reason'] + " Invalid GSTIN/UIN of Recipient."

            # ....................Invoice Type........................................
            df.loc[(df['Invoice Type'].astype(str).str.lower() == 'na') | (df['Invoice Type'].isna()), 'reason'] = df[
                                                                                                                       'reason'] + " Invoice Type should not be blank."
            df.loc[(((df['Invoice Type'].astype(str).str.lower() != 'b2b') & (
                        df['Invoice Type'].astype(str).str.lower() != 'b2bur') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'cnr') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'cnur') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'dnr') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'dnur') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'impg') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'imps')) & (
                                (df['Invoice Type'].astype(str).str.lower() != 'na') & (
                            df['Invoice Type'].notna()))), 'reason'] = df[
                                                                           'reason'] + " Invoice Type should be B2B,B2BUR,IMPS,IMPG,CNR,DRN,CNUR,DNUR."
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

            # .....................Invoice Nmuber...................................
            df.loc[((df['Invoice No'].astype(str).str.lower() == 'na') | (df['Invoice No'] == 0) | (
                df['Invoice No'].isna())), 'reason'] = df['reason'] + " Invoice No should not be blank."

            # .....................Invoice Date...................................
            df['Invoice Date'] = df['Invoice Date'].apply(func)
            df.loc[(df['Invoice Date'].isnull()), 'reason'] = df['reason'] + " Invoice Date is Incorrect."
            df.loc[(df['Invoice Date'].astype(str).str.lower() == 'na') | (df['Invoice Date'] == 0), 'reason'] = df[
                                                                                                                     'reason'] + " Invoice Date should not be blank."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice Date'].transform(
                                                                                 lambda x: x != x.iloc[0]) & (df[
                                                                                                                  'Invoice Date'].notna())].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice Date must be same for same Invoice No.'
            df.loc[(pd.to_datetime('today') < df['Invoice Date']), 'reason'] = df[
                                                                                   'reason'] + " Date should not be greater then current date."
            df.loc[(~(pd.datetime.now().date() - pd.DateOffset(months=18) < df['Invoice Date']) & (
                        (df['Invoice Date'].notna()) & (df['Invoice Date'] != 'na'))), 'reason'] = df[
                                                                                                       'reason'] + " Invoice Date should not be 18 months older."
            df.loc[("2017-07-01" > df['Invoice Date'].astype(str)), 'reason'] = df[
                                                                                    'reason'] + " Invoice Date should be After 01-JULY-2017."

            # .....................Seller GSTIN..................................
            df.loc[((df['Seller Gstin'].astype(str).str.lower() == 'na') | (df['Seller Gstin'].astype(str) == '0') | (
                df['Seller Gstin'].isna())), 'reason'] = df['reason'] + " Seller GSTIN should not be blank."
            df.loc[(((df['Seller Gstin'].astype(str).str.lower() != 'na') & (df['Seller Gstin'].astype(str) != '0') & (
                df['Seller Gstin'].notna()) & (df['Seller Gstin'].astype(str).str.len() != 15))), 'reason'] = df[
                                                                                                                  'reason'] + " Max size of Seller GSTIN No Should be 15."
            df.loc[((df['Seller Gstin'].astype(str).str.match(
                "[0-9]{2}[a-zA-Z]{5}[0-9]{4}[a-zA-Z]{1}[1-9A-Za-z]{1}[Z]{1}[0-9a-zA-Z]{1}") == False) & (
                                (df['Seller Gstin'].astype(str).astype(str).str.lower() != 'na') & (
                                    df['Seller Gstin'].astype(str) != '0') & (df['Seller Gstin'].notna()))), 'reason'] = \
            df['reason'] + " Invalid Seller GSTIN."

            # .................................Invoice value............................
            df.loc[((df['Invoice Value'] == 0) | (df['Invoice Value'].astype(str).str.lower() == 'na') | (
                df['Invoice Value'].isna())), 'reason'] = df['reason'] + " Invoice Value should not be blank."
            df.loc[~((df['Invoice Value'].astype(str).str.replace('.', '').str.isnumeric()) & (
                        (df['Invoice Value'] != 0) & (
                            df['Invoice Value'].astype(str).astype(str).str.lower() != 'na') & (
                            df['Invoice Value'].notna()))), 'reason'] = df[
                                                                            'reason'] + " Invoice Value should be numeric."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice Value'].transform(
                                                                                 lambda x: x != x.iloc[0]) & (
                                                                                         (df['Invoice No'] != 0) & (
                                                                                             df['Invoice No'].astype(
                                                                                                 str).astype(
                                                                                                 str).str.lower() != 'na') & (
                                                                                             df[
                                                                                                 'Invoice Value'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice Value must be same for same Invoice No.'

            # ++++++++++++++++++++++++++++++++++++++Common for all invoices+++++++++++++++++++++++++++++++++++++++++++
            return df

        # +++++++++++++++++++++++++++++++++B2B validation++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        def B2B_validation_purchase(b2b):
            df = b2b
            df = df.reset_index()
            # .....................Amendment......................................
            #     df.loc[df.set_index(['Seller Gstin','Buyer Gstin','Invoice No']).index.isin(d.set_index(['sellerGSTIN','buyerGSTIN','oldInvoiceNo']).index),'reason']=' You have already created Amendmend'
            #     df.loc[df.set_index(['Seller Gstin','Buyer Gstin','Invoice No']).index.isin(d.set_index(['sellerGSTIN','buyerGSTIN','refrInvoiceNo']).index),'reason']=df['reason']+' You have already created Credit/Debit'

            # .....................Seller GSTIN..................................
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Seller Gstin'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Seller Gstin must be same for same Invoice No.'

            # .........................Buyer GSTIN.....................................
            df.loc[(df['Buyer Gstin'] == df['Seller Gstin']) & (df['Seller Gstin'].notna()), 'reason'] = df[
                                                                                                             'reason'] + " Supplier and Buyer GSTIN should not be same."

            # ............................pos validation.........................................
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # .....................Seller Company Name......................................
            if ('Seller Company Name' in list(df.columns)):
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[df.groupby(
                        df['Invoice No'].astype(str))['Seller Company Name'].transform(lambda x: x != x.iloc[0]) & ((df[
                                                                                                                         'Invoice No'].astype(
                        str).astype(str).str.lower() != 'na') & (df['Invoice No'] != 0) & (df[
                                                                                               'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Seller Company Name must be same for same Invoice No.'

            # ............................Eligibility For ITC.........................................

            if ('Eligibility For ITC' in list(df.columns)):
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'inputs') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'ip')), 'Eligibility For ITC'] = "ip"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'capital goods') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'cp')), 'Eligibility For ITC'] = "cp"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'input services') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'is')), 'Eligibility For ITC'] = "is"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'ineligible') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'no')), 'Eligibility For ITC'] = "no"

            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')
            return df

        # ++++++++++++++++++++++++++++++++++++B2BUR purchase+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        def B2BUR_validation_purchase(b2bur):
            df = b2bur
            df = df.reset_index()

            # .................................Taxable Value............................
            df['Taxable Value'] = df['Taxable Value'].apply(pd.to_numeric, lambda x: '%.2f' % x, errors='ignore')
            df.loc[((df['Taxable Value'] == 0) | (df['Taxable Value'].astype(str).astype(str).str.lower() == 'na') | (
                df['Taxable Value'].isna())), 'reason'] = df['reason'] + " Taxable Value should not be blank."
            df.loc[~(df['Taxable Value'].astype(str).str.replace('.', '').str.isnumeric()) & (
                df['Taxable Value'].notna()), 'reason'] = df['reason'] + " Taxable Value should be numeric."

            # ............................pos validation.........................................
            df.loc[(df['Place Of Supply'].astype(str).str.lower() == 'na'), 'reason'] = df[
                                                                                            'reason'] + " Place Of Supply should not be blank."
            df.loc[(df['Place Of Supply'].isna()), 'reason'] = df['reason'] + " Invalid Place Of Supply."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # .....................................Rate.................................
            df.loc[~(df['Rate'].astype(str).str.replace('.', '').str.isnumeric()) & (df['Rate'].notna()), 'reason'] = \
            df['reason'] + " Rate should be numeric."
            #     df.loc[(df['Rate'].astype(str).astype(str).str.lower()!='na')&(~((df['Rate'].astype(str)=="0.0")|(df['Rate'].astype(str)=='0.25')|(df['Rate'].astype(str)=='0.10')|(df['Rate'].astype(str)=='3.0')|(df['Rate'].astype(str)=='5.0')|(df['Rate'].astype(str)=='12.0')|(df['Rate'].astype(str)=='18.0')|(df['Rate'].astype(str)=='28.0')|(df['Rate'].astype(str)=="0")|(df['Rate'].astype(str)=='0.25')|(df['Rate'].astype(str)=='0.10')|(df['Rate'].astype(str)=='3')|(df['Rate'].astype(str)=='5')|(df['Rate'].astype(str)=='12')|(df['Rate'].astype(str)=='18')|(df['Rate'].astype(str)=='28'))),'reason']=df['reason']+" Rate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."

            # .....................Seller Company Name......................................
            if ('Seller Company Name' in list(df.columns)):
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[df.groupby(
                        df['Invoice No'].astype(str))['Seller Company Name'].transform(lambda x: x != x.iloc[0]) & ((df[
                                                                                                                         'Invoice No'].astype(
                        str).astype(str).str.lower() != 'na') & (df['Invoice No'] != 0) & (df[
                                                                                               'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Seller Company Name must be same for same Invoice No.'

            # ............................Eligibility For ITC.........................................

            if ('Eligibility For ITC' in list(df.columns)):
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'inputs') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'ip')), 'Eligibility For ITC'] = "ip"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'capital goods') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'cp')), 'Eligibility For ITC'] = "cp"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'input services') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'is')), 'Eligibility For ITC'] = "is"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'ineligible') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'no')), 'Eligibility For ITC'] = "no"

            # Final data conversion in dd-mm-yy format........
            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        # ++++++++++++++++++++++++++++++++++++++++++IMPS validation++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        def IMPS_validation_purchase(imps):
            df = imps
            df = df.reset_index()

            # .................................Taxable Value............................
            df['Taxable Value'] = df['Taxable Value'].apply(pd.to_numeric, lambda x: '%.2f' % x, errors='ignore')
            df.loc[(df['Taxable Value'] == 0) | (df['Taxable Value'].isna()), 'reason'] = df[
                                                                                              'reason'] + " Taxable Value should not be blank."
            df.loc[~(df['Taxable Value'].astype(str).str.replace('.', '').str.isnumeric()) & (
                df['Taxable Value'].notna()), 'reason'] = df['reason'] + " Taxable Value should be numeric."

            # ............................pos validation.........................................
            df.loc[
                ((df['Place Of Supply'].astype(str).str.lower() == 'na') | (df['Place Of Supply'].isna())), 'reason'] = \
            df['reason'] + " Place Of Supply should not be blank."

            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # .....................................Rate.................................
            df.loc[~(df['Rate'].astype(str).str.replace('.', '').str.isnumeric()) & (df['Rate'].notna()), 'reason'] = \
            df['reason'] + " Rate should be numeric."

            # .....................Seller Company Name......................................

            if ('Seller Company Name' in list(df.columns)):
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[df.groupby(
                        df['Invoice No'].astype(str))['Seller Company Name'].transform(lambda x: x != x.iloc[0]) & ((df[
                                                                                                                         'Invoice No'].astype(
                        str).astype(str).str.lower() != 'na') & (df['Invoice No'] != 0) & (df[
                                                                                               'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Seller Company Name must be same for same Invoice No.'

            # ............................Eligibility For ITC.........................................

            if ('Eligibility For ITC' in list(df.columns)):
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'input services') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'is')), 'Eligibility For ITC'] = "is"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'ineligible') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'no')), 'Eligibility For ITC'] = "no"

            # Final data conversion in dd-mm-yy format........
            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        # +++++++++++++++++++++++++++++++++++++++++IMPG validation++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        def IMPG_validation_purchase(impg):
            df = impg
            df = df.reset_index()
            print(
                "##############################################################   INSIDE IMPG TYPE    ###########################################################################")
            print(df['Invoice Type'], "df data of invoice type")

            # #....................Invoice Subtype.....................................
            # df.loc[((df['Invoice SubType'].astype(str).astype(str).str.lower()!='na')&(df['Invoice SubType']!=0)),'reason']=df['reason']+" Invoice SubType should not be blank."
            # df.loc[(((df['Invoice SubType'].astype(str).str.lower()!='imports')&(df['Invoice SubType'].astype(str).str.lower()!='received from sez'))&((df['Invoice SubType'].astype(str).astype(str).str.lower()!='na')&(df['Invoice SubType']!=0))),'reason']=df['reason']+" Enter Document Type either 'Imports' or 'Received from SEZ'."
            # if df.empty == True:
            #     pass
            # else:
            #     df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[df.groupby(df['Invoice No'].astype(str))['Invoice SubType'].transform(lambda x: x!= x.iloc[0])&((df['Invoice No'].astype(str).astype(str).str.lower()!='na')&(df['Invoice No']!=0))].unique())).any(1),'reason']=df['reason']+' Document Type must be same for same Bill of Entry Number.'

            # ....................Invoice Type.....................................
            df.loc[((df['Invoice Type'].astype(str).str.lower() == 'na') | (df['Invoice Type'] == 0) | (
                df['Invoice Type'].isna())), 'reason'] = df['reason'] + " Invoice Type should not be blank."
            df.loc[(((df['Invoice Type'].astype(str).str.lower() != 'imports') & (
                        df['Invoice Type'].astype(str).str.lower() != 'received from sez') & (
                                 df['Invoice Type'].astype(str).str.lower() != 'impg')) & (
                                (df['Invoice Type'].astype(str).astype(str).str.lower() != 'na') & (
                                    df['Invoice Type'] != 0) & (df['Invoice Type'].notna()))), 'reason'] = df[
                                                                                                               'reason'] + " Enter Invoice Type either 'Imports' or 'Received from SEZ'."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Invoice Type'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Invoice Type must be same for same Bill of Entry Number.'

            # .................................Taxable Value............................
            df['Taxable Value'] = df['Taxable Value'].apply(pd.to_numeric, lambda x: '%.2f' % x, errors='ignore')
            df.loc[((df['Taxable Value'] == 0) | (df['Taxable Value'].astype(str).astype(str).str.lower() == 'na') | (
                df['Taxable Value'].isna())), 'reason'] = df['reason'] + " Taxable Value should not be blank."
            df.loc[~(df['Taxable Value'].astype(str).str.replace('.', '').str.isnumeric()) & (
                df['Taxable Value'].notna()), 'reason'] = df['reason'] + " Taxable Value should be numeric."

            # .....................................Rate.................................
            df.loc[~(df['Rate'].astype(str).str.replace('.', '').str.isnumeric()) & (df['Rate'].notna()), 'reason'] = \
            df['reason'] + " Rate should be numeric."
            #     df.loc[(df['Rate'].astype(str).astype(str).str.lower()!='na')&(~((df['Rate'].astype(str)=="0.0")|(df['Rate'].astype(str)=='0.25')|(df['Rate'].astype(str)=='0.10')|(df['Rate'].astype(str)=='3.0')|(df['Rate'].astype(str)=='5.0')|(df['Rate'].astype(str)=='12.0')|(df['Rate'].astype(str)=='18.0')|(df['Rate'].astype(str)=='28.0')|(df['Rate'].astype(str)=="0")|(df['Rate'].astype(str)=='0.25')|(df['Rate'].astype(str)=='0.10')|(df['Rate'].astype(str)=='3')|(df['Rate'].astype(str)=='5')|(df['Rate'].astype(str)=='12')|(df['Rate'].astype(str)=='18')|(df['Rate'].astype(str)=='28'))),'reason']=df['reason']+" Rate should be Equal either of 0 / 0.25 / 0.10 / 3 / 5 / 12 / 18 / 28."

            # .....................Seller Company Name......................................
            if ('Seller Company Name' in list(df.columns)):
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[df.groupby(
                        df['Invoice No'].astype(str))['Seller Company Name'].transform(lambda x: x != x.iloc[0]) & ((df[
                                                                                                                         'Invoice No'].astype(
                        str).astype(str).str.lower() != 'na') & (df['Invoice No'] != 0) & (df[
                                                                                               'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Seller Company Name must be same for same Invoice No.'

            # .....................Seller Gstin..................................
            if ('Seller Gstin' in list(df.columns)):
                df.loc[((df['Invoice SubType'].astype(str).str.lower() == 'received from sez') & (
                            (df['Seller Gstin'].astype(str).astype(str).str.lower() != 'na') & (
                                df['Seller Gstin'] != 0) & (df['Seller Gstin'].notna()))), 'reason'] = df[
                                                                                                           'reason'] + " GSTIN of SEZ Supplier should not be blank for Document Type 'Received from SEZ'."
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[df.groupby(
                        df['Invoice No'].astype(str))['Seller Gstin'].transform(lambda x: x != x.iloc[0]) & ((df[
                                                                                                                  'Invoice No'].astype(
                        str).astype(str).str.lower() != 'na') & (df['Invoice No'] != 0) & (df[
                                                                                               'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' GSTIN of SEZ Supplier must be same for same Invoice No.'

            # ............................Eligibility For ITC.........................................

            if ('Eligibility For ITC' in list(df.columns)):
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'inputs') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'ip')), 'Eligibility For ITC'] = "ip"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'capital goods') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'cp')), 'Eligibility For ITC'] = "cp"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'ineligible') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'no')), 'Eligibility For ITC'] = "no"

            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        # +++++++++++++++++++++++++++++++++++CDNR validation+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

        def CDNR_validation_purchase(cdnr):
            df = cdnr
            df = df.reset_index()

            # .....................Seller GSTIN..................................
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Seller Gstin'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Seller Gstin must be same for same Invoice No.'

            # .........................Buyer GSTIN.....................................
            df.loc[((df['Buyer Gstin'] == df['Seller Gstin']) & (df['Seller Gstin'].notna())), 'reason'] = df[
                                                                                                               'reason'] + " Supplier and Buyer GSTIN should not be same."

            # .....................Reference Invoice No...................................
            df.loc[((df['Reference Invoice No'].astype(str).str.lower() == 'na') | (df['Reference Invoice No'] == 0) | (
                df['Reference Invoice No'].isna())), 'reason'] = df[
                                                                     'reason'] + " Invoice/Advance Payment Voucher No should not be blank."

            # .....................Reference Invoice Date.........................
            df['Reference Invoice Date'] = df['Reference Invoice Date'].apply(func)
            df.loc[(df['Reference Invoice Date'].isnull()), 'reason'] = df[
                                                                            'reason'] + " Invoice/Advance Payment Voucher  date is Incorrect."
            df.loc[((df['Reference Invoice Date'].astype(str).str.lower() == 'na') | (
                        df['Reference Invoice Date'] == 0) | (df['Reference Invoice Date'].isna())), 'reason'] = df[
                                                                                                                     'reason'] + " Invoice/Advance Payment Voucher date should not be blank."
            df.loc[("2017-07-01" > df['Reference Invoice Date'].astype(str)), 'reason'] = df[
                                                                                              'reason'] + " Invoice Date should be After 01-JULY-2017."
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Reference Invoice Date'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df[
                                        'reason'] + ' Invoice/Advance Payment Voucher date must be same for same Note No.'

            # ............................pos validation.........................................
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # .....................Seller Company Name......................................
            if ('Seller Company Name' in list(df.columns)):
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[df.groupby(
                        df['Invoice No'].astype(str))['Seller Company Name'].transform(lambda x: x != x.iloc[0]) & ((df[
                                                                                                                         'Invoice No'].astype(
                        str).astype(str).str.lower() != 'na') & (df['Invoice No'] != 0) & (df[
                                                                                               'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Seller Company Name must be same for same Invoice No.'

            # ............................Eligibility For ITC.........................................
            if ('Eligibility For ITC' in list(df.columns)):
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'inputs') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'ip')), 'Eligibility For ITC'] = "ip"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'capital goods') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'cp')), 'Eligibility For ITC'] = "cp"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'input services') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'is')), 'Eligibility For ITC'] = "is"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'ineligible') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'no')), 'Eligibility For ITC'] = "no"

            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        # +++++++++++++++++++++++++++++++++++CDNR validation+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        def CDNUR_validation_purchase(cdnur):
            df = cdnur
            df = df.reset_index()
            # .....................Seller GSTIN..................................
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Seller Gstin'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Seller Gstin must be same for same Invoice No.'

            # .........................Buyer GSTIN.....................................
            df.loc[((df['Buyer Gstin'] == df['Seller Gstin']) & (df['Seller Gstin'].notna())), 'reason'] = df[
                                                                                                               'reason'] + " Supplier and Buyer GSTIN should not be same."

            # .....................................Rate.................................
            df.loc[~(df['Rate'].astype(str).str.replace('.', '').str.isnumeric()) & (df['Rate'].notna()), 'reason'] = \
            df['reason'] + " Rate should be numeric."

            # .....................Reference Invoice No...................................
            df.loc[((df['Reference Invoice No'].astype(str).str.lower() == 'na') | (df['Reference Invoice No'] == 0) | (
                df['Reference Invoice No'].isna())), 'reason'] = df[
                                                                     'reason'] + " Invoice/Advance Payment Voucher No should not be blank."

            # .....................Reference Invoice Date.........................
            df['Reference Invoice Date'] = df['Reference Invoice Date'].apply(func)
            df.loc[(df['Reference Invoice Date'].isnull()), 'reason'] = df[
                                                                            'reason'] + " Invoice/Advance Payment Voucher  date is Incorrect."
            df.loc[((df['Reference Invoice Date'].astype(str).str.lower() == 'na') | (
                        df['Reference Invoice Date'] == 0)), 'reason'] = df[
                                                                             'reason'] + " Invoice/Advance Payment Voucher date should not be blank."
            df.loc[("2017-07-01" > df['Reference Invoice Date'].astype(str)), 'reason'] = df[
                                                                                              'reason'] + " Invoice Date should be After 01-JULY-2017."
            df.loc[~((df['Invoice Date'] < df['Reference Invoice Date']) & (
                        df['Reference Invoice No'].astype(str) == df['Invoice No'].astype(str))), 'reason'] = df[
                                                                                                                  'reason'] + " Note/Refund Voucher date should be after Invoice date for same Invoice/Advance Receipt Number & Note/Refund Voucher Number."
            df.loc[~(df['Invoice Date'].astype(str) <= df['Reference Invoice Date'].astype(str)), 'reason'] = df[
                                                                                                                  'reason'] + " Note/Refund Voucher date should be after or equal to the invoice date."

            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Reference Invoice Date'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df[
                                        'reason'] + ' Invoice/Advance Payment Voucher date must be same for same Note No.'

            # ............................pos validation.........................................
            if df.empty == True:
                pass
            else:
                df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[
                                                                             df.groupby(df['Invoice No'].astype(str))[
                                                                                 'Place Of Supply'].transform(
                                                                                 lambda x: x != x.iloc[0]) & ((df[
                                                                                                                   'Invoice No'].astype(
                                                                                 str).astype(
                                                                                 str).str.lower() != 'na') & (df[
                                                                                                                  'Invoice No'] != 0) & (
                                                                                                                  df[
                                                                                                                      'Invoice No'].notna()))].unique())).any(
                    1), 'reason'] = df['reason'] + ' Place Of Supply must be same for same Invoice No.'

            # .....................Seller Company Name......................................
            if ('Seller Company Name' in list(df.columns)):
                if df.empty == True:
                    pass
                else:
                    df.loc[pd.DataFrame(df['Invoice No'].tolist()).isin(list(df['Invoice No'].loc[df.groupby(
                        df['Invoice No'].astype(str))['Seller Company Name'].transform(lambda x: x != x.iloc[0]) & ((df[
                                                                                                                         'Invoice No'].astype(
                        str).astype(str).str.lower() != 'na') & (df['Invoice No'] != 0) & (df[
                                                                                               'Invoice No'].notna()))].unique())).any(
                        1), 'reason'] = df['reason'] + ' Seller Company Name must be same for same Invoice No.'

            # ............................Eligibility For ITC.........................................
            if ('Eligibility For ITC' in list(df.columns)):
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'inputs') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'ip')), 'Eligibility For ITC'] = "ip"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'capital goods') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'cp')), 'Eligibility For ITC'] = "cp"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'input services') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'is')), 'Eligibility For ITC'] = "is"
                df.loc[((df['Eligibility For ITC'].astype(str).str.lower() == 'ineligible') | (
                            df['Eligibility For ITC'].astype(str).str.lower() == 'no')), 'Eligibility For ITC'] = "no"

            df['Invoice Date'] = df['Invoice Date'].dt.strftime('%d-%m-%Y')

            return df

        df_final_s = pd.DataFrame()
        for j in ls:
            s = len(str(j))
            print(s)
            print(j, 'Buyer GSTIN')
            df_f = stage_tb.loc[stage_tb['Buyer Gstin'] == j]
            df_f['reason'] = ''

            # df_f = df_f.reset_index()
            df3 = pd.DataFrame()
            df21 = pd.DataFrame()
            df31 = pd.DataFrame()
            df32 = pd.DataFrame()
            if (s > 9):
                if (j[2:-3] == pan_num):
                    print("PAN match", j[2:-3])
                    print("PAN from Angular", pan_num)
                    df = Common_check_purchase(df_f)
                    ############################################# AUTO POPULATING FINANCIAL PERIOD and YEAR##########################################################
                    #             df.loc[((df['Invoice Date'].dt.month) <= 4), 'invoiceFinancialPeriod'] = ((df['Invoice Date'].dt.year - 1).fillna(0).astype(int)).map(str) + "-" + ((df['Invoice Date'].dt.year).fillna(0).astype(int)).map(str)
                    #             df.loc[((df['Invoice Date'].dt.month) > 4), 'invoiceFinancialPeriod'] = ((df['Invoice Date'].dt.year).fillna(0).astype(int)).map(str) + "-" + ((df['Invoice Date'].dt.year + 1).fillna(0).astype(int)).map(str)
                    #             df['financialPeriod'] = dataUploadEngine['month2']
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
                    if (j[2:-3] != pan_num):
                        print(" I am in else condtion", j)
                        print("PAN from Angular", pan_num)
                        df_f['reason'] = df_f['reason'] + 'Invalid PAN number for GSTIN'
                        df31 = df_f
            else:
                df_f['reason'] = df_f['reason'] + 'Invalid Buyer GSTIN'
                df32 = df_f

            df_final_s = df_final_s.append([df3, df31, df32], sort=True)

        df_final_s['Status'] = np.where((df_final_s['reason'] == ''), 'Success', 'Fail')

        return df_final_s