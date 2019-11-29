# import pandas as pd
# import numpy as np
# from pyxlsb import open_workbook
# import pymysql
# from sqlalchemy import create_engine
# import os
# import r
# import time
import os
from .views import *
from .filereading import Filere
# from .views import fileread
import pandas as pd
import numpy as np
from operator import *
from pyxlsb import open_workbook
from .models import *
# import pymysql, os, re, time
from sqlalchemy import create_engine
def purchase_engine(df, rule):
    print("ruler mastewr:- ", rule.columns)
    print("ruler123:- ", rule['master_id'])
    df.info()
    ls2 = df.columns.tolist()
    for i in ls2:
        e = df[i].dtype.name
        if e == 'float64':
            df[i] = df[i].fillna(0)
            df[i] = df[i].astype(str).replace({r'^\b(?:NA|NaN|Blank|nan)\b$': 0, r'^[\s\-.]*$': 0}, regex=True).apply(pd.to_numeric, errors='coerce').fillna(0)
        elif e == 'int64':
            df[i] = df[i].fillna(0)
            df[i] = df[i].astype(str).replace({r'^\b(?:NA|NaN|Blank|nan)\b$': 0, r'^[\s\-.]*$': 0}, regex=True).apply(
                pd.to_numeric, errors='coerce').fillna(0)
        elif e == 'object':
            df[i] = df[i].fillna("na")
            df[i] = df[i].astype(str).replace({r'^\b(?:NA|NaN|Blank|nan)\b$': "na", r'^[\s\-.]*$': "na"},regex=True).fillna("na")

    ops = {'plus': add,
           'sub': sub,
           'concat': concat,
           'mul': mul,
           'equal': eq,
           'notequal': ne,
           'and': and_,
           'or': or_,
           'less': lt,
           'concat': add,
           'greater': gt,
           'greater-equal': ge,
           'less-equal': le
           }

    def RuleDemo(size, Rule, d):

        # ...................... 1 Rule.................................................................................................

        if (size == 3):
            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = float(Rule[2]) if Rule[2].lstrip('-').replace('.', '').isnumeric() else Rule[2]

            def Dym(col1, opr1, val1, d):

                data = In[d].loc[(ops[opr1](In[d][col1], val1))]

                return data

            data = Dym(col1, opr1, val1, d)


        # ...................... 2 Rule.................................................................................................
        elif (size == 7):
            print("I am in 7")
            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = float(Rule[2]) if Rule[2].lstrip('-').replace('.', '').isnumeric() else Rule[2]
            print('This is val 1', val1)
            print(type(val1))
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = float(Rule[6]) if Rule[6].lstrip('-').replace('.', '').isnumeric() else Rule[6]
            print('This is val 2', val2)
            print(type(val2))
            if ((log1 == 'and') or (log1 == 'or')):
                # ......................2 and  or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, d):
                    data = In[d].loc[(ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, d)


        # ...................... 3 Rule.................................................................................................
        # (df['Rate'].astype(str).str.lstrip('-').str.replace('.','').str.isnumeric())
        elif (size == 11):
            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]

            if ((log1 == 'and' and log2 == 'and') or (log1 == 'or' and log2 == 'or')):

                # ......................2 and / 2 or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, d):
                    data = In[d].loc[(
                        ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                                  (ops[opr3](In[d][col3], val3))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, d)


            elif ((log1 == 'and') and (log2 == 'or')):

                # ......................1 and  1 or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, d):

                    data = In[d].loc[(ops[log1]((ops[opr1](In[d][col1], val1)), (
                        ops[log2]((ops[opr2](In[d][col2], val2)), (ops[opr3](In[d][col3], val3))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, d)


        # ...................... 4 Rule.................................................................................................

        elif (size == 15):

            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]
            log3 = Rule[11]
            col4 = Rule[12]
            opr4 = Rule[13]
            val4 = int(Rule[14]) if Rule[14].isnumeric() else Rule[14]

            if ((log1 == 'and' and log2 == 'and' and log3 == 'and') or (
                    log1 == 'or' and log2 == 'or' and log3 == 'or')):

                # ......................3 and / 3 or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d):

                    data = In[d].loc[(ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[opr4](In[d][col4], val4))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d)

            elif ((log1 == 'and' and log2 == 'and' and log3 == 'or')):

                # ......................2 and  1 or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d):

                    data = In[d].loc[(
                        ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                                  (ops[log3]((ops[opr3](In[d][col3], val3)), (ops[opr4](In[d][col4], val4))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d)


            elif ((log1 == 'and' and log2 == 'or' and log3 == 'or')):

                # ......................1 and  2 or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d):

                    data = In[d].loc[(ops[log1]((ops[opr1](In[d][col1], val1)), (
                        ops[log3]((ops[log2]((ops[opr2](In[d][col2], val2)), (ops[opr3](In[d][col3], val3)))),
                                  (ops[opr4](In[d][col4], val4))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d)
            # =====================================================================================================================

            elif ((log1 == 'or' and log2 == 'and' and log3 == 'or')):

                # ......................1 and  2 or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d):

                    data = In[d].loc[(ops[log2]((
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2))))),
                        (ops[log3]((ops[opr3](In[d][col3], val3)), (ops[opr4](In[d][col4], val4))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d)

        # =====================================================================================================================

        # ...................... 5 Rule.................................................................................................

        elif (size == 19):

            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]
            log3 = Rule[11]
            col4 = Rule[12]
            opr4 = Rule[13]
            val4 = int(Rule[14]) if Rule[14].isnumeric() else Rule[14]
            log4 = Rule[15]
            col5 = Rule[16]
            opr5 = Rule[17]
            val5 = int(Rule[18]) if Rule[18].isnumeric() else Rule[18]
            if ((log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'and') or (
                    log1 == 'or' and log2 == 'or' and log3 == 'or' and log4 == 'or')):

                # ......................4 and  or...................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, d):

                    data = In[d].loc[(ops[log4]((ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[opr4](In[d][col4], val4)))),
                        (ops[opr5](In[d][col5], val5))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, d)

            elif ((log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'or')):

                # ......................3 and  1 or...................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, d):

                    data = In[d].loc[(ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[log4]((ops[opr4](In[d][col4], val4)),
                                                                     (ops[opr5](In[d][col5], val5))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, d)

            elif ((log1 == 'and' and log2 == 'and' and log3 == 'or' and log4 == 'or')):

                # ......................2 and  2 or...................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, d):

                    data = In[d].loc[(
                        ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))), (
                            ops[log4]((ops[log3]((ops[opr3](In[d][col3], val3)), (ops[opr4](In[d][col4], val4)))),
                                      (ops[opr5](In[d][col5], val5))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, d)

            elif ((log1 == 'and' and log2 == 'or' and log3 == 'or' and log4 == 'or')):

                # ......................1 and  3 or...................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, d):

                    data = In[d].loc[(ops[log1]((ops[opr1](In[d][col1], val1)), (ops[log4]((ops[log3](
                        (ops[log2]((ops[opr2](In[d][col2], val2)), (ops[opr3](In[d][col3], val3)))),
                        (ops[opr4](In[d][col4], val4)))), (ops[opr5](In[d][col5], val5))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, d)

            # =====================================================================================================================
            elif ((log1 == 'or' and log2 == 'or' and log3 == 'and' and log4 == 'or')):

                # ......................1 and  3 or...................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, d):

                    data = In[d].loc[(ops[log3]((ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                                      (ops[opr2](In[d][col2], val2)))),
                                                           (ops[opr3](In[d][col3], val3)))),

                                                (ops[log4]((ops[opr4](In[d][col4], val4)),
                                                           (ops[opr5](In[d][col5], val5))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, d)

            # =====================================================================================================================

            elif ((log1 == 'or' and log2 == 'and' and log3 == 'or' and log4 == 'or')):

                # ......................1 and  3 or...................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, d):

                    data = In[d].loc[(ops[log2](((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                            (ops[opr2](In[d][col2], val2))))),
                                                (ops[log4]((ops[log3]((ops[opr3](In[d][col3], val3)),
                                                                      (ops[opr4](In[d][col4], val4)))),
                                                           (ops[opr5](In[d][col5], val5))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, d)

        # =====================================================================================================================

        # ...................... 6 Rule....................................................................................

        elif (size == 23):

            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]
            log3 = Rule[11]
            col4 = Rule[12]
            opr4 = Rule[13]
            val4 = int(Rule[14]) if Rule[14].isnumeric() else Rule[14]
            log4 = Rule[15]
            col5 = Rule[16]
            opr5 = Rule[17]
            val5 = int(Rule[18]) if Rule[18].isnumeric() else Rule[18]
            log5 = Rule[19]
            col6 = Rule[20]
            opr6 = Rule[21]
            val6 = int(Rule[22]) if Rule[22].isnumeric() else Rule[22]

            if ((log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'and' and log5 == 'and') or (
                    log1 == 'or' and log2 == 'or' and log3 == 'or' and log4 == 'or' and log5 == 'or')):

                # ......................5 and / 5 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(ops[log5]((ops[log4]((ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[opr4](In[d][col4], val4)))),
                        (ops[opr5](In[d][col5], val5)))),
                        (ops[opr6](In[d][col6], val6))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)


            elif ((log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'and' and log5 == 'or')):

                # ......................4 and  1 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(ops[log4]((ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[opr4](In[d][col4], val4)))), (
                        ops[log5]((ops[opr5](In[d][col5], val5)),
                                  (ops[opr6](In[d][col6], val6))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)


            elif ((log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'or' and log5 == 'or')):

                # ......................3 and  2 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[log5](
                        (ops[log4]((ops[opr4](In[d][col4], val4)), (ops[opr5](In[d][col5], val5)))),
                        (ops[opr6](In[d][col6], val6))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)


            elif ((log1 == 'and' and log2 == 'and' and log3 == 'or' and log4 == 'or' and log5 == 'or')):

                # ......................2 and  3 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(
                        ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))), (
                            ops[log5]((ops[log4](
                                (ops[log3]((ops[opr3](In[d][col3], val3)), (ops[opr4](In[d][col4], val4)))),
                                (ops[opr5](In[d][col5], val5)))), (ops[opr6](In[d][col6], val6))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)


            elif ((log1 == 'and' and log2 == 'and' and log3 == 'or' and log4 == 'or' and log5 == 'or')):

                # ......................1 and  4 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(ops[log1]((ops[opr1](In[d][col1], val1)), (ops[log5]((ops[log4]((ops[log3](
                        (ops[log2]((ops[opr2](In[d][col2], val2)), (ops[opr3](In[d][col3], val3)))),
                        (ops[opr4](In[d][col4], val4)))), (ops[opr5](In[d][col5], val5)))), (ops[opr6](In[d][col6],
                                                                                                       val6))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)

            # =====================================================================================================================

            elif ((log1 == 'or' and log2 == 'or' and log3 == 'or' and log4 == 'and' and log5 == 'or')):

                # ......................1 and  4 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(ops[log4](ops[log3]((ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                                                (ops[opr2](In[d][col2], val2)))),
                                                                     (ops[opr3](In[d][col3], val3)))),
                                                          (ops[opr4](In[d][col4], val4))),
                                                (ops[log5]((ops[opr5](In[d][col5], val5)),
                                                           (ops[opr6](In[d][col6], val6))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)


            # =====================================================================================================================

            elif ((log1 == 'and' and log2 == 'or' and log3 == 'or' and log4 == 'or' and log5 == 'or')):

                # ......................1 and  4 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    # #                 data=In[d].loc[(ops[log5]((ops[log4]((ops[log3]((ops[log2]((ops[log1](
                    #                                   (ops[opr1](In[d][col1], val1)),(ops[opr2](In[d][col2], val2)))),
                    #                                    (ops[opr3](In[d][col3], val3)))),(ops[opr4](In[d][col4], val4)))),
                    #                                    (ops[opr5](In[d][col5], val5)))),(ops[opr6](In[d][col6], val6))))]

                    #                 data=In[d].loc[(ops[log1]((ops[opr1](In[d][col1],val1)),
                    #                                 (ops[log3]((ops[log2]((ops[opr2](In[d][col2],val2)),
                    #                                                     (ops[opr3] (In[d][col3],val3)))),
                    #                                                     (ops[opr4](In[d][col4],val4))))))]

                    data = In[d].loc[(ops[log1]((ops[opr1](In[d][col1], val1)),
                                                (ops[log5](
                                                    (ops[log4]((ops[log3]((ops[log2]((ops[opr2](In[d][col2], val2)),
                                                                                     (ops[opr3](In[d][col3], val3)))),
                                                                          (ops[opr4](In[d][col4], val4)))),
                                                               (ops[opr5](In[d][col5], val5)))),
                                                    (ops[opr6](In[d][col6], val6))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)


            # =====================================================================================================================

            elif ((log1 == 'or' and log2 == 'or' and log3 == 'and' and log4 == 'or' and log5 == 'or')):

                # ......................1 and  4 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(ops[log3]((ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                                      (ops[opr2](In[d][col2], val2)))),
                                                           (ops[opr3](In[d][col3], val3)))),
                                                ops[log5]((ops[log4]((ops[opr4](In[d][col4], val4)),
                                                                     (ops[opr5](In[d][col5], val5)))),
                                                          (ops[opr6](In[d][col6], val6)))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)

            # =====================================================================================================================
            elif ((log1 == 'or' and log2 == 'and' and log3 == 'or' and log4 == 'or' and log5 == 'or')):

                # ......................1 and  4 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(ops[log2](((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                            (ops[opr2](In[d][col2], val2))))),
                                                (ops[log5]((ops[log4]((ops[log3]((ops[opr3](In[d][col3], val3)),
                                                                                 (ops[opr4](In[d][col4], val4)))),
                                                                      (ops[opr5](In[d][col5], val5)))),
                                                           (ops[opr6](In[d][col6], val6))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)

            # =====================================================================================================================

            elif ((log1 == 'or' and log2 == 'and' and log3 == 'or' and log4 == 'and' and log5 == 'or')):

                # ......................1 and  4 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):

                    data = In[d].loc[(ops[log4]((ops[log2]((
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2))))),
                        (ops[log3]((ops[opr3](In[d][col3], val3)), (ops[opr4](In[d][col4], val4)))))),
                        (ops[log5]((ops[opr5](In[d][col5], val5)), (ops[opr6](In[d][col6], val6))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)

        # =====================================================================================================================

        # ......................................... 7 Rule.............................................................................

        elif (size == 27):

            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]
            log3 = Rule[11]
            col4 = Rule[12]
            opr4 = Rule[13]
            val4 = int(Rule[14]) if Rule[14].isnumeric() else Rule[14]
            log4 = Rule[15]
            col5 = Rule[16]
            opr5 = Rule[17]
            val5 = int(Rule[18]) if Rule[18].isnumeric() else Rule[18]
            log5 = Rule[19]
            col6 = Rule[20]
            opr6 = Rule[21]
            val6 = int(Rule[22]) if Rule[22].isnumeric() else Rule[22]
            log6 = Rule[23]
            col7 = Rule[24]
            opr7 = Rule[25]
            val7 = int(Rule[26]) if Rule[26].isnumeric() else Rule[26]

            if (
                    (
                            log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'and' and log5 == 'and' and log6 == 'or')):

                # ......................5 and  1 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[(ops[log5]((ops[log4]((ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[opr4](In[d][col4], val4)))),
                        (ops[opr5](In[d][col5], val5)))), (
                        ops[log6]((ops[opr6](In[d][col6], val6)),
                                  (ops[opr7](In[d][col7], val7))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)


            elif (
                    (
                            log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'and' and log5 == 'or' and log6 == 'or')):

                # ......................4 and  2 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[(ops[log4]((ops[log3]((ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                                                 (ops[opr2](In[d][col2], val2)))),
                                                                      (ops[opr3](In[d][col3], val3)))),
                                                           (ops[opr4](In[d][col4], val4)))),
                                                (ops[log6]((ops[log5]((ops[opr5](df[col5], val5)),
                                                                      (ops[opr6](In[d][col6], val6)))),
                                                           (ops[opr7](In[d][col7], val7))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)



            elif (
            (log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'or' and log5 == 'or' and log6 == 'or')):
                print("Inside 27th and 3 and & 3 or")

                # ......................3 and  3 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):
                    print("&&&|||")
                    data = In[d].loc[(ops[log3]((ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                                      (ops[opr2](In[d][col2], val2)))),
                                                           (ops[opr3](In[d][col3], val3)))),
                                                (ops[log6]((ops[log5]((ops[log4]((ops[opr4](In[d][col4], val4)),
                                                                                 (ops[opr5](In[d][col5], val5)))),
                                                                      (ops[opr6](In[d][col6], val6)))),
                                                           (ops[opr7](In[d][col7], val7))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)



            elif (
                    (
                            log1 == 'and' and log2 == 'and' and log3 == 'or' and log4 == 'or' and log5 == 'or' and log6 == 'or')):

                # ......................2 and  4 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[(
                        ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))), (
                            ops[log6]((ops[log5]((ops[log4](
                                (ops[log3]((ops[opr3](In[d][col3], val3)), (ops[opr4](In[d][col4], val4)))),
                                (ops[opr5](In[d][col5], val5)))), (ops[opr6](In[d][col6], val6)))),
                                (ops[opr7](In[d][col7], val7))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)



            elif ((log1 == 'and' and log2 == 'or' and log3 == 'or' and log4 == 'or' and log5 == 'or' and log6 == 'or')):

                # ......................1 and  5 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[(ops[log1]((ops[opr1](In[d][col1], val1)), (ops[log6]((ops[log5]((ops[log4]((ops[
                        log3](
                        (ops[log2]((ops[opr2](In[d][col2], val2)), (ops[opr3](In[d][col3], val3)))),
                        (ops[opr4](In[d][col4], val4)))), (ops[opr5](In[d][col5], val5)))), (ops[opr6](In[d][col6],
                                                                                                       val6)))), (
                        ops[opr7](In[d][col7],
                                  val7))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)


            elif ((
                          log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'and' and log5 == 'and' and log6 == 'and') | (
                          log1 == 'or' and log2 == 'or' and log3 == 'or' and log4 == 'or' and log5 == 'or' and log6 == 'or')):

                # .....................6 and / 6 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[(ops[log6]((ops[log5]((ops[log4]((ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[opr4](In[d][col4], val4)))),
                        (ops[opr5](In[d][col5], val5)))),
                        (ops[opr6](In[d][col6], val6)))),
                        (ops[opr7](In[d][col7], val7))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)
            # =====================================================================================================================

            elif ((log1 == 'or' and log2 == 'or' and log3 == 'or' and log4 == 'or' and log5 == 'and' and log6 == 'or')):

                # ......................1 and  5 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[
                        (ops[log5]((ops[log4]((ops[log3]((ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                                               (ops[opr2](In[d][col2], val2)))),
                                                                    (ops[opr3](In[d][col3], val3)))),
                                                         (ops[opr4](In[d][col4], val4)))),
                                              (ops[opr5](In[d][col5], val5)))),
                                   (ops[log6]((ops[opr6](In[d][col6], val6)), (ops[opr7](In[d][col7], val7))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)


            # =====================================================================================================================
            elif ((log1 == 'or' and log2 == 'and' and log3 == 'or' and log4 == 'or' and log5 == 'or' and log6 == 'or')):

                # ......................1 and  5 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[(ops[log2]((
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2))))),
                        (ops[log6]((ops[log5]((ops[log4]((ops[log3]((ops[opr3](In[d][col3], val3)),
                                                                    (ops[opr4](In[d][col4], val4)))),
                                                         (ops[opr5](In[d][col5], val5)))),
                                              (ops[opr6](In[d][col6], val6)))), (ops[opr7](In[d][col7], val7))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)


            # =====================================================================================================================
            elif (
                    (
                            log1 == 'or' and log2 == 'or' and log3 == 'and' and log4 == 'or' and log5 == 'and' and log6 == 'or')):

                # ......................1 and  5 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[(ops[log5]((ops[log3]((ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)),
                                                                                 (ops[opr2](In[d][col2], val2)))),
                                                                      (ops[opr3](In[d][col3], val3)))),

                                                           (ops[log4]((ops[opr4](In[d][col4], val4)),
                                                                      (ops[opr5](In[d][col5], val5)))))),

                                                (ops[log6]((ops[opr6](In[d][col6], val6)),
                                                           (ops[opr7](In[d][col7], val7))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)


            # =====================================================================================================================
            elif (
                    (
                            log1 == 'or' and log2 == 'and' and log3 == 'or' and log4 == 'or' and log5 == 'and' and log6 == 'or')):

                # ......................1 and  5 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):

                    data = In[d].loc[(ops[log5](ops[log2]((
                        (ops[log1]((ops[opr1](In[d][col1], val1)),
                                   (ops[opr2](In[d][col2], val2))))),

                        (ops[log4]((ops[log3]((ops[opr3](In[d][col3], val3)),
                                              (ops[opr4](In[d][col4], val4)))),
                                   (ops[opr5](In[d][col5], val5))))),

                        (ops[log6]((ops[opr6](In[d][col6], val6)),
                                   (ops[opr7](In[d][col7], val7))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)


            # =====================================================================================================================
            elif (
            (log1 == 'or' and log2 == 'and' and log3 == 'or' and log4 == 'and' and log5 == 'or' and log6 == 'or')):

                # ......................1 and  5 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):
                    data = In[d].loc[(ops[log4](ops[log2]((
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2))))),
                        (ops[log3]((ops[opr3](In[d][col3], val3)), (ops[opr4](In[d][col4], val4))))),
                        (ops[log6]((ops[log5]((ops[opr5](In[d][col5], val5)),
                                              (ops[opr6](In[d][col6], val6)))),
                                   (ops[opr7](In[d][col7], val7))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)

        # =====================================================================================================================

        return data

    def DemoB2CS(size, Rule, d):

        if (size == 11):
            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]

            if ((log1 == 'and') and (log2 == 'or')):
                # ......................1 and  1 or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, d):
                    data = In[d].loc[(
                        ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                                  (ops[opr3](In[d][col3], val3))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, d)



        elif (size == 15):

            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]
            log3 = Rule[11]
            col4 = Rule[12]
            opr4 = Rule[13]
            val4 = int(Rule[14]) if Rule[14].isnumeric() else Rule[14]

            if ((log1 == 'and' and log2 == 'and' and log3 == 'or')):
                # ......................2 and  1 or.................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d):
                    data = In[d].loc[(ops[log1]((ops[opr1](In[d][col1], val1)), (
                        ops[log3]((ops[log2]((ops[opr2](In[d][col2], val2)), (ops[opr3](In[d][col3], val3)))),
                                  (ops[opr4](In[d][col4], val4))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, d)

        elif (size == 19):

            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]
            log3 = Rule[11]
            col4 = Rule[12]
            log4 = Rule[15]
            col5 = Rule[16]
            opr5 = Rule[17]
            val5 = int(Rule[18]) if Rule[18].isnumeric() else Rule[18]

            if ((log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'or')):
                # ......................3 and  1 or...................................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, d):
                    data = In[d].loc[(
                        ops[log2]((ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))), (
                            ops[log4]((ops[log3]((ops[opr3](In[d][col3], val3)), (ops[opr4](In[d][col4], val4)))),
                                      (ops[opr5](In[d][col5], val5))))))]
                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, d)


        elif (size == 23):

            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]
            log3 = Rule[11]
            col4 = Rule[12]
            opr4 = Rule[13]
            val4 = int(Rule[14]) if Rule[14].isnumeric() else Rule[14]
            log4 = Rule[15]
            col5 = Rule[16]
            opr5 = Rule[17]
            val5 = int(Rule[18]) if Rule[18].isnumeric() else Rule[18]
            log5 = Rule[19]
            col6 = Rule[20]
            opr6 = Rule[21]
            val6 = int(Rule[22]) if Rule[22].isnumeric() else Rule[22]

            if ((log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'and' and log5 == 'or')):
                # ......................4 and  1 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, d):
                    data = In[d].loc[(ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](In[d][col1], val1)), (ops[opr2](In[d][col2], val2)))),
                        (ops[opr3](In[d][col3], val3)))), (ops[log5](
                        (ops[log4]((ops[opr4](In[d][col4], val4)), (ops[opr5](In[d][col5], val5)))),
                        (ops[opr6](In[d][col6], val6))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, d)


        elif (size == 27):
            col1 = Rule[0]
            opr1 = Rule[1]
            val1 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            log1 = Rule[3]
            col2 = Rule[4]
            opr2 = Rule[5]
            val2 = int(Rule[6]) if Rule[6].isnumeric() else Rule[6]
            log2 = Rule[7]
            col3 = Rule[8]
            opr3 = Rule[9]
            val3 = int(Rule[10]) if Rule[10].isnumeric() else Rule[10]
            log3 = Rule[11]
            col4 = Rule[12]
            opr4 = Rule[13]
            val4 = int(Rule[14]) if Rule[14].isnumeric() else Rule[14]
            log4 = Rule[15]
            col5 = Rule[16]
            opr5 = Rule[17]
            val5 = int(Rule[18]) if Rule[18].isnumeric() else Rule[18]
            log5 = Rule[19]
            col6 = Rule[20]
            opr6 = Rule[21]
            val6 = int(Rule[22]) if Rule[22].isnumeric() else Rule[22]
            log6 = Rule[23]
            col7 = Rule[24]
            opr7 = Rule[25]
            val7 = int(Rule[26]) if Rule[26].isnumeric() else Rule[26]
            if (
            (log1 == 'and' and log2 == 'and' and log3 == 'and' and log4 == 'and' and log5 == 'and' and log6 == 'or')):
                # ......................5 and  1 or ....................................................

                def Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4, log4,
                        col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d):
                    data = df.loc[(ops[log4]((ops[log3]((ops[log2](
                        (ops[log1]((ops[opr1](df[col1], val1)), (ops[opr2](df[col2], val2)))),
                        (ops[opr3](df[col3], val3)))), (ops[opr4](df[col4], val4)))), (ops[log6](
                        (ops[log5]((ops[opr5](df[col5], val5)), (ops[opr6](df[col6], val6)))),
                        (ops[opr7](df[col7], val7))))))]

                    return data

                data = Dym(col1, opr1, val1, log1, col2, opr2, val2, log2, col3, opr3, val3, log3, col4, opr4, val4,
                           log4, col5, opr5, val5, log5, col6, opr6, val6, log6, col7, opr7, val7, d)

        return data

    ##########################################Funcation########################################################

    def ConcatD(size, Rule, d, New_column):

        if (size == 1):
            col1 = Rule[0]

            def ConD(d, col1, New_column):
                In[d][New_column] = In[d][col1].map(str)
                return In[d]

            data = ConD(d, col1, New_column)

        elif (size == 2):
            col1 = Rule[0]
            col2 = Rule[1]

            def ConD(d, col1, col2, New_column):
                In[d][New_column] = In[d][col1].map(str) + In[d][col2].map(str)
                return In[d]

            data = ConD(d, col1, col2, New_column)

        elif (size == 3):
            col1 = Rule[0]
            col2 = Rule[1]
            col3 = Rule[2]

            def ConD(d, col1, col2, col3, New_column):
                In[d][New_column] = In[d][col1].map(str) + In[d][col2].map(str) + In[d][col3].map(str)
                return In[d]

            data = ConD(d, col1, col2, col3, New_column)

        elif (size == 4):
            col1 = Rule[0]
            col2 = Rule[1]
            col3 = Rule[2]
            col4 = Rule[3]

            def ConD(d, col1, col2, col3, col4, New_column):
                In[d][New_column] = In[d][col1].map(str) + In[d][col2].map(str) + In[d][col3].map(str) + In[d][
                    col4].map(str)
                return In[d]

            data = ConD(d, col1, col2, col3, col4, New_column)


        elif (size == 5):
            col1 = Rule[0]
            col2 = Rule[1]
            col3 = Rule[2]
            col4 = Rule[3]
            col5 = Rule[4]

            def ConD(d, col1, col2, col3, col4, col5, New_column):
                In[d][New_column] = In[d][col1].map(str) + In[d][col2].map(str) + In[d][col3].map(str) + In[d][
                    col4].map(str) + In[d][col5].map(str)
                return In[d]

            data = ConD(d, col1, col2, col3, col4, col5, New_column)

        return data

    # +=========================================Mew Addded funcation===================================================================

    def StartWith(d, new_column, size, Values, Set_value, Rule):
        col1 = Rule[0]
        print("Inside Startwith")
        print("In StartWith: 2. New _Column - ", new_column, "\n 3. size - ", size, "\n 4. Rule - ", Rule,
              "\n 5. Set_value - ", Set_value, "\n 6. Values - ", Values)
        # limit = int(Rule[1]) if Rule[1].isnumeric() else Rule[1]
        limit = int(Values)

        def StartW12(d, new_column, col1, limit):
            In[d][new_column] = In[d][col1].astype(str).str[:limit]
            #         print(new_column)
            return In[d]

        data = StartW12(d, new_column, col1, limit)
        return data

    def EndWith(d, new_column, Values, Rule):
        col1 = Rule[0]
        limit = int(Values)

        def EndW12(d, new_column, col1, limit):
            In[d][new_column] = In[d][col1].str[limit:]
            #         print(new_column)
            return In[d]

        data = EndW12(d, new_column, col1, limit)
        return data

    def Roundoff(d, new_column, size, Rule):
        col1 = Rule[0]

        def Roundoff12(d, new_column, col1):
            In[d][new_column] = In[d][col1].apply(lambda x: np.round(x, decimals=2))
            print(new_column)
            return In[d]

        data = Roundoff12(d, new_column, col1)
        return data

    def Arithmatic(size, Rule, d, new_column):
        if (size == 3):
            col1 = Rule[0]
            col2 = Rule[1]
            col3 = int(Rule[2]) if Rule[2].isnumeric() else Rule[2]
            if (col2 == "Add"):
                In[d][new_column] = In[d][col1] + col3
                return In[d]

            elif (col2 == 'Sub'):
                In[d][new_column] = In[d][col1] - col3
                return In[d]

            elif (col2 == 'Mul'):
                In[d][new_column] = In[d][col1] * col3
                return In[d]

            elif (col2 == 'Div'):
                In[d][new_column] = In[d][col1] / col3
                return In[d]

        elif (size == 5):

            col1 = Rule[0]
            col2 = Rule[1]
            col3 = Rule[2]
            col4 = Rule[3]
            col5 = int(Rule[4]) if Rule[4].isnumeric() else Rule[4]

            if (col2 == "Add" and col4 == "Div"):
                In[d][new_column] = (In[d][col1] + In[d][col3]) / col5
                return In[d]

            elif (col2 == "Add" and col4 == "Sub"):
                In[d][new_column] = (In[d][col1] + In[d][col3]) - col5
                return In[d]

            elif (col2 == "Add" and col4 == "Mul"):
                In[d][new_column] = (In[d][col1] + In[d][col3]) * col5
                return In[d]

            elif (col2 == "Sub" and col4 == "Add"):
                In[d][new_column] = (In[d][col1] - In[d][col3]) + col5
                return In[d]

            elif (col2 == "Sub" and col4 == "Mul"):
                In[d][new_column] = (In[d][col1] - In[d][col3]) * col5
                return In[d]

            elif (col2 == "Sub" and col4 == "Div"):
                In[d][new_column] = (In[d][col1] - In[d][col3]) / col5
                return In[d]

            elif (col2 == "Mul" and col4 == "Add"):
                In[d][new_column] = (In[d][col1] * In[d][col3]) + col5
                return In[d]


            elif (col2 == "Mul" and col4 == "Div"):
                print(col1, col3)
                In[d][new_column] = (In[d][col1] * In[d][col3]) / col5
                return In[d]


            elif (col2 == "Mul" and col4 == "Sub"):
                In[d][new_column] = (In[d][col1] * In[d][col3]) - col5
                return In[d]


            elif (col2 == "Div" and col4 == "Add"):
                In[d][new_column] = (In[d][col1] / In[d][col3]) + col5
                return In[d]


            elif (col2 == "Div" and col4 == "Mul"):
                In[d][new_column] = (In[d][col1] / In[d][col3]) * col5
                return In[d]


            elif (col2 == "Div" and col4 == "Sub"):
                In[d][new_column] = (In[d][col1] / In[d][col3]) - col5
                return In[d]

    def Abs(size, Rule, d, new_column):
        print("Inside ABS:- ", d)
        print("Inside ABS 2:- ", In[d])
        if (size == 1):
            col1 = Rule[0]
            In[d][new_column] = In[d][col1].abs()
            return In[d]

    def Lenght(size, Rule, d, new_column):
        if (size == 1):
            col1 = Rule[0]
            In[d][new_column] = In[d][col1].str.len()
            return In[d]

    def Set_Val(d, Set_value, new_column):
        print(new_column)
        In[d][new_column] = Set_value
        return In[d]

    def Populate(d, Rule, new_column):
        col1 = Rule[0]

        def Set_col(d, col1, new_column):
            print(new_column)
            In[d][new_column] = In[d][col1]
            In[d][new_column] = In[d][new_column].replace(['na', '0', 0], '')
            return In[d]

        data = Set_col(d, col1, new_column)
        return data

    def Sum(size, Rule, d, new_column):

        if (size == 2):
            col1 = Rule[0]
            col2 = Rule[1]
            print(col1, col2)
            In[d][col1] = In[d][col1].astype(float)
            In[d][col2] = In[d][col2].astype(float)

            def Sumval(d, col1, col2, new_column):
                print(col1, col2)
                In[d][new_column] = In[d][[col1, col2]].sum(axis=1)

                # In[d][new_column]=In[d][col1].astype(float)+In[d][col2].astype(float)

                return In[d]

            data = Sumval(d, col1, col2, new_column)


        elif (size == 3):
            col1 = Rule[0]
            col2 = Rule[1]
            col3 = Rule[2]
            In[d][col1] = In[d][col1].astype(float)
            In[d][col2] = In[d][col2].astype(float)
            In[d][col3] = In[d][col3].astype(float)

            def Sumval(d, col1, col2, new_column):
                In[d][new_column] = In[d][[col1, col2, col3]].sum(axis=1)
                return In[d]

            data = Sumval(d, col1, col2, new_column)


        elif (size == 4):
            print("I am in 4 sum")

            col1 = Rule[0]
            col2 = Rule[1]
            col3 = Rule[2]
            col4 = Rule[3]
            In[d][col1] = In[d][col1].astype('float64')
            In[d][col2] = In[d][col2].astype('float64')
            In[d][col3] = In[d][col3].astype('float64')
            In[d][col4] = In[d][col4].astype('float64')

            def Sumval(d, col1, col2, col3, col4, new_column):
                In[d][new_column] = In[d][[col1, col2, col3, col4]].sum(axis=1)
                return In[d]

            data = Sumval(d, col1, col2, col3, col4, new_column)
            print(data['M_Invoice_Value'])


        elif (size == 5):
            col1 = Rule[0]
            col2 = Rule[1]
            col3 = Rule[2]
            col4 = Rule[3]
            col5 = Rule[4]

            In[d][col1] = In[d][col1].astype(float)
            In[d][col2] = In[d][col2].astype(float)
            In[d][col3] = In[d][col3].astype(float)
            In[d][col4] = In[d][col4].astype(float)
            In[d][col5] = In[d][col5].astype(float)

            def Sumval(d, col1, col2, col3, col4, col5, new_column):
                In[d][new_column] = In[d][[col1, col2, col3, col4, col5]].sum(axis=1)
                return In[d]

            data = Sumval(d, col1, col2, col3, col4, col5, new_column)


        elif (size == 6):
            col1 = Rule[0]
            col2 = Rule[1]
            col3 = Rule[2]
            col4 = Rule[3]
            col5 = Rule[4]
            col6 = Rule[5]

            In[d][col1] = In[d][col1].astype(float)
            In[d][col2] = In[d][col2].astype(float)
            In[d][col3] = In[d][col3].astype(float)
            In[d][col4] = In[d][col4].astype(float)
            In[d][col5] = In[d][col5].astype(float)
            In[d][col6] = In[d][col6].astype(float)

            def Sumval(d, col1, col2, col3, col4, col5, col6, new_column):
                In[d][new_column] = In[d][[col1, col2, col3, col4, col5, col6]].sum(axis=1)
                return In[d]

            data = Sumval(d, col1, col2, col3, col4, col5, col6, new_column)

        return data

    def MasterFile(d, df2, new_column, Rule, Set_value):
        Set_value = [x for xs in [Set_value] for x in xs.split(',')]
        print("Inside MAster :-- ", df2)
        print("Inside Master 2 :-- ", d)
        length = int(len(Rule) / 2)
        Source = Rule[:length]
        Master = Rule[length:]
        print(Master, Set_value)
        Ms_col = Master + Set_value
        print(Ms_col)
        In[d] = pd.merge(In[d], df2[Ms_col], left_on=Source, right_on=Master, how='left')
        return In[d]

    # ms=MasterFile(d,df2,new_column,Rule,Set_value)

    def ExcelFile(d, new_column, Rule):
        print(Rule, "Rule")
        # length = int(len(Rule) / 2)
        # Source1 = ','.join(map(str, Rule[:length]))
        # Source2 = ','.join(map(str, Rule[length:]))
        # print(Source1, Source2)
        # In[d][new_column] = In[d][Source1].astype(str).str.lower() == In[d][Source2].astype(str).str.lower()
        # In[d][new_column] = In[d][new_column].astype(str).str.lower()
        # return In[d]

        col1 = Rule[0]
        opr1 = Rule[1]  # def ExcelFile(d,new_column,Rule):
        col2 = Rule[2]  # length=int(len(Rule)/2)
        In[d][new_column] = 'No'
        In[d].loc[
            (ops[opr1](In[d][col1], In[d][col2])), new_column] = 'Yes'  # Source1=','.join(map(str, Rule[:length]))
        print(In[d][new_column], "true -false test")
        return In[d]  # Source2=','.join(map(str, Rule[length:]))

    #     print(Source1,Source2)
    #     In[d][new_column]=In[d][Source1].astype(str)==In[d][Source2].astype(str)
    #     return In[d]

    # ===================================================================================================================
    def LineItem(d, Rule, new_column):
        col1 = Rule[0]
        col2 = Rule[1]

        def lineitem(d, col1, col2, new_column):
            print(new_column)
            In[d][col2] = In[d][col2].astype(str).str.lower()
            In[d][new_column] = In[d][[col1]].sum(axis=1).groupby(In[d][col2]).transform('sum')
            In[d][new_column] = In[d][new_column].fillna(0).astype(float)
            return In[d]

        data = lineitem(d, col1, col2, new_column)

        return data

    # ===================================================================================================================

    def State_Master(d, Rule, new_column):
        col1 = Rule[0]

        def State_Master1(d, col1, new_column):
            state_master_dict = {
                '01-Jammu and Kashmir': ['jammu and kashmir', 'jammu & kashmir', 'jammu and kashmir', 'j&k', 'jk',
                                         '01-jammu & kashmir', '01-jammu and kashmir', '01', '1', 1],
                '02-Himachal Pradesh': ['himachal pradesh', 'himachal pradesh', 'hp', '02-himachal pradesh', '02', '2',
                                        2],
                '03-Punjab': ['punjab', 'panjab', 'pb', '03-punjab', '03', '3', 3],
                '04-Chandigarh': ['chandigarh', 'chandigarh', '04-chandigarh', 'ch', '04', '4', 4],
                '05-Uttarakhand': ['uttarakhand', 'uttarakhand', 'uk', '05-uttarakhand', '05', '5', 5],
                '06-Haryana': ['haryana', 'haryana', 'hr', '06-haryana', 'haryana cr card', 'haryanaÂ  cr card',
                               'haryanaa  cr card', '06', '6', 6],
                '07-Delhi': ['delhi', 'delhi', 'dl', 'new delhi', 'new-delhi', 'newdelhi', 'new_delhi', 'dl',
                             '07-delhi', '07-new delhi', '07-new-delhi', '07', '7', 7],
                '08-Rajasthan': ['rajasthan', 'rajsthan', 'rj', 'rajasthan', '08-rajasthan', '08', '8', 8],
                '09-Uttar Pradesh': ['uttar pradesh', 'uttar-pradesh', 'uttar_pradesh', 'up', 'utar pradesh',
                                     '09-uttar pradesh', '09', '9', 9],
                '10-Bihar': ['bihar', 'bhihar', 'br', '10-bihar', '10', 10],
                '11-Sikkim': ['sikkim', 'sikkhim', 'sikkim', 'sk', '11-sikkim', '11', 11],
                '12-Arunachal Pradesh': ['arunachal pradesh', 'arunachal-pradesh', 'arunachal_pradesh',
                                         'arunachal pradesh', 'ap', '12-arunachal pradesh', '12', 12],
                '13-Nagaland': ['nagaland', 'nagaland', 'nl', '13-nagaland', '13', 13],
                '14-Manipur': ['manipur', 'manipur', 'mn', '14-manipur', '14', 14],
                '15-Mizoram': ['mizoram', 'mizoram', 'mz', '15-mizoram', '15', 15],
                '16-Tripura': ['tripura', 'tripura', 'tr', '16-tripura', '16', 16],
                '17-Meghalaya': ['meghalaya', 'meghalaya', 'ml', '17-meghalaya', '17', 17],
                '18-Assam': ['assam', 'assam', 'as', '18-assam', '18', 18],
                '19-West Bengal': ['west bengal', 'west-bengal', 'west_bengal', 'west bengal', 'wb', '19-west bengal',
                                   19, '19'],
                '20-Jharkhand': ['jharkhand', 'jharkhand', 'jk', '20-jharkhand', '20', 20],
                '21-Odisha': ['odisha', 'odisa', 'orissa', 'od', 'or', 'odisha', '21-odisha', '21', 21],
                '22-Chhattisgarh': ['chhattisgarh', 'chattisgarh', 'cg', 'ct', 'chhattisgarh', '22-chhattisgarh', '22',
                                    22],
                '23-Madhya Pradesh': ['madhya pradesh', 'madhya_pradesh', 'madhya-pradesh', 'mp', 'madhya pradesh',
                                      '23-madhya pradesh', '23', 23],
                '24-Gujarat': ['gujarat', 'gujarat', 'gujraat', 'gj', 'gujrat', '24-gujarat', '24', 24],
                '25-Daman and Diu': ['daman & diu', 'daman and diu', 'diu & daman', 'dd', 'diu and daman',
                                     '25-daman and diu', '25-daman & diu', '25', 25],
                '26-Dadra & Nagar Haveli': ['dadra & nagar haveli', 'dadra and nagar haveli', 'dadra and nagar haveli',
                                            'dn', '26-dadra & nagar haveli', '26', 26],
                '27-Maharashtra': ['maharashtra', 'maharastra', 'mh', 'maharashtra', '27-maharashtra',
                                   'digital banking', '27', 27],
                '29-Karnataka': ['karnataka', 'karnataka', 'ka', '29-karnataka', '29', 29],
                '30-Goa': ['goa', 'goa', 'ga', '30-goa', '30', 30],
                '31-Lakshdweep': ['lakshdweep', 'lakshadweep islands', 'ld', 'lakshdweep', '31-lakshdweep', '31', 31],
                '32-Kerala': ['kerala', 'kerala', 'kl', '32-kerala', '32', 32],
                '33-Tamil Nadu': ['tamil nadu', 'tamil-nadu', 'tamil_nadu', 'tamilnadu', 'tamil nadu', 'tn',
                                  '33-tamil nadu', '33', 33],
                '34-Pondicherry': ['pondicherry', 'pondicherry', 'py', '34-pondicherry', 'puducherry', '34', 34],
                '35-Andaman & Nicobar': ['andaman & nicobar islands', 'andaman and nicobar islands',
                                         'andaman & nicobar', 'andaman and nicobar islands', 'andaman and nicobar',
                                         'an', '35-andaman & nicobar islands', '35', 35],
                '36-Telengana': ['telengana', 'telangana', 'ts', 'telengana', '36-telengana', '36-telangana', '36', 36],
                '37-Andhra Pradesh': ['andhra pradesh', 'andhra_pradesh', 'andhra-pradesh', 'andhrapradesh', 'ad', 'ap',
                                      '37-andhra pradesh', 37, 28, '37', '28'],
                '97-Other Territory': ['other territory', 'other-territory', 'other_territory', 'otherterritory', 'oth',
                                       '97-other territory', 97, '97']}

            def argcontains(item):
                for i, v in state_master_dict.items():
                    if item in v:
                        return i

            print(new_column)
            In[d][new_column] = In[d][col1].astype(str).str.lower().map(argcontains)
            return In[d]

        data = State_Master1(d, col1, new_column)
        return data

    def ComaparePopulate(d, Rule, new_column, Set_value):
        print(Rule)
        col1 = Rule[0]
        opr1 = Rule[1]
        val1 = float(Rule[2]) if Rule[2].lstrip('-').replace('.', '').isnumeric() else Rule[2]

        #     print(Rule[0])
        def ComPop(d, col1, opr1, val1, Set_value, new_column):
            print(opr1, new_column, Set_value)
            In[d].loc[(ops[opr1](In[d][col1], val1)), new_column] = In[d][Set_value]
            In[d][new_column] = In[d][new_column].replace(['na', '0', 0], '')
            return In[d]

        data = ComPop(d, col1, opr1, val1, Set_value, new_column)
        return data

    def Set_Duplicates_Flags(d, Rule, new_column):
        print(Rule)
        print("Type", Rule)
        print(Rule[0])
        print(Rule[1])
        col1 = Rule[0]
        col2 = Rule[1]
        print("This is col1 and col2", col1)
        In[d][new_column] = In[d][[col1, col2]].duplicated(keep=False)
        return In[d]

    # rule = pd.read_excel(r"E:\Dynamic_Excel\ThyssenKrupp Rules.xlsx",sheetname='Sheet1')
    print(rule, "here is rule data")
    rule = rule.replace('null', np.nan)
    rule = rule.fillna('')
    #     rule.rename(index = {"Values": "values"},inplace = True)

    rule['values'] = rule['values'].astype(str)

    rule['rule'] = rule[['header_col1', 'oprator1', 'header_col2', 'oprator2', 'values', 'connector']].apply(
        lambda x: ','.join(i for i in x if (i != '')), axis=1)

    d = rule.groupby('rule_id')['rule'].agg(lambda x: ','.join(x)).reset_index()

    # rule['set_value']=rule.groupby('rule_id')['set_value'].transform('sum')

    rule = pd.merge(left=rule, right=d[['rule_id', 'rule']], on=['rule_id'], how='left').drop_duplicates(
        subset=['rule_id', 'rule_y'], keep='last')

    del rule['rule_x']

    rule.rename({'rule_y': 'Rule'}, axis=1, inplace=True)
    # rule.to_csv("D:/abc.csv")

    # ++++++++++++++++++++++++++++++++++Rule calling++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    print(rule)
    last_row = len(rule)
    print(last_row)
    for i in range(last_row):
        Rule = rule.iloc[i]['Rule']
        # print(type(Rule))
        print("Rule----", Rule)
        d = rule.iloc[i]['data_in']
        print("D - Value:- ", d)
        Column_value = rule.iloc[i]['column_value']
        print("Column_value-----", Column_value)
        Rule = str(Rule)
        Rule = Rule.split(',')
        size = len(Rule)
        print("size----", size)
        print("Rule Val:- ", Rule)
        new_column = rule.iloc[i]['new_column']
        print("new_column----", new_column)
        Set_value = rule.iloc[i]['set_value']
        print("Set_value---------", Set_value)
        Values = rule.iloc[i]['values']
        print("Values  -  ", Values)
        # print(df,"before if")
        log = ''
        if (Set_value == 'B2CS'):
            if (size == 11):
                log = Rule[7]
            elif (size == 15):
                log = Rule[11]
            elif (size == 19):
                log = Rule[15]
            elif (size == 23):
                log = Rule[19]
            elif (size == 27):
                log = Rule[23]

        if ((Column_value == 'comparison') and (Set_value == 'B2CS') and (log == 'or')):
            In = {'df': df, }
            Invoice_type = DemoB2CS(size, Rule, d)

            if (rule.iloc[i]['new_column'] in df.columns):
                df.loc[df.index.intersection(Invoice_type.index), rule.iloc[i]['new_column']] = float(Set_value) if str(
                    Set_value).isnumeric() else Set_value

            else:
                print(rule.iloc[i]['new_column'])
                df[rule.iloc[i]['new_column']] = ''
                df.loc[df.index.intersection(Invoice_type.index), rule.iloc[i]['new_column']] = float(Set_value) if str(
                    Set_value).isnumeric() else Set_value

        elif ((Column_value == 'comparison')):
            In = {'df': df, }
            Invoice_type = RuleDemo(size, Rule, d)

            if (rule.iloc[i]['new_column'] in df.columns):
                df.loc[df.index.intersection(Invoice_type.index), rule.iloc[i]['new_column']] = float(Set_value) if str(
                    Set_value).isnumeric() else Set_value

            else:
                print(rule.iloc[i]['new_column'])
                df[rule.iloc[i]['new_column']] = ''
                df.loc[df.index.intersection(Invoice_type.index), rule.iloc[i]['new_column']] = float(Set_value) if str(
                    Set_value).isnumeric() else Set_value

        #     elif((Column_value=='comparison')and(Set_value=='B2CS')):
        #         In={'df':df,}
        #         Invoice_type=DemoB2CS(size,Rule,d)

        #         if(rule.iloc[i]['new_column'] in df.columns):
        #             df.loc[df.index.intersection(Invoice_type.index),rule.iloc[i]['new_column']]=Set_value

        #         else:
        #             print(rule.iloc[i]['new_column'])
        #             df[rule.iloc[i]['new_column']]=''
        #             df.loc[df.index.intersection(Invoice_type.index),rule.iloc[i]['new_column']]=Set_value

        elif (Column_value == 'CONCAT'):
            In = {'df': df, }
            df = ConcatD(size, Rule, d, new_column)
            # print(d1)
            # df=con

        elif (Column_value == 'Startwith'):
            In = {'df': df, }
            df = StartWith(d, new_column, size, Values, Set_value, Rule)
            # df=Startwith1

        elif (Column_value == 'Endwith'):
            In = {'df': df, }
            df = EndWith(d, new_column, Set_value, Rule)

        elif (Column_value == 'Roundoff'):
            In = {'df': df, }
            df = Roundoff(d, new_column, size, Rule)

        elif (Column_value == 'Length'):
            In = {'df': df, }
            df = Lenght(size, Rule, d, new_column)

        elif (Column_value == 'Abs'):
            In = {'df': df, }
            df = Abs(size, Rule, d, new_column)

        elif (Column_value == 'Arithmetic'):
            In = {'df': df, }
            df = Arithmatic(size, Rule, d, new_column)

        elif (Column_value == 'Sum'):
            In = {'df': df, }
            df = Sum(size, Rule, d, new_column)

        elif (Column_value == 'Populate'):
            In = {'df': df, }
            df = Populate(d, Rule, new_column)

        elif (Column_value == 'Master col'):
            In = {'df': df, }
            print("ruler DDDDD:", rule['master_id'])
            Master_val = list(Mastertable.objects.filter(id = rule['master_id']).values('file','sheet_name','row_no'))
            print(Master_val,"Master_val")
            # filename=os.path.basename(Master_val[0]['file'])
            # print(filename,"filename")
            fileExtension = Master_val[0]['file'].split(os.extsep)[-1]
            d1=Filere()
            df_data = d1.fileread(fileExtension, Master_val[0]['sheet_name'], Master_val[0]['file'], Master_val[0]['row_no'])
            print(df_data,"final")
            df = MasterFile(d, df_data, new_column, Rule, Set_value)

        elif (Column_value == 'Excel col'):
            In = {'df': df, }
            df = ExcelFile(d, new_column, Rule)

        elif (Column_value == 'Set_Val'):
            In = {'df': df, }
            df = Set_Val(d, Set_value, new_column)

        elif (Column_value == 'LineItem'):
            In = {'df': df, }
            df = LineItem(d, Rule, new_column)

        elif (Column_value == 'State_Master'):
            In = {'df': df, }
            df = State_Master(d, Rule, new_column)

        elif (Column_value == 'ComaparePopulate'):
            In = {'df': df, }
            df = ComaparePopulate(d, Rule, new_column, Set_value)

        elif (Column_value == 'Set_Duplicates_Flags'):
            In = {'df': df, }
            df = Set_Duplicates_Flags(d, Rule, new_column)
        # time.sleep(3)

    return df