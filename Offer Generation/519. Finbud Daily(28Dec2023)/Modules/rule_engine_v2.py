
##############################################################
#                 ███╗░░██╗██╗██████╗░░█████╗░               #
#                 ████╗░██║██║██╔══██╗██╔══██╗               #
#                 ██╔██╗██║██║██████╔╝██║░░██║               #
#                 ██║╚████║██║██╔══██╗██║░░██║               #
#                 ██║░╚███║██║██║░░██║╚█████╔╝               #
#                 ╚═╝░░╚══╝╚═╝╚═╝░░╚═╝░╚════╝░               #
##############################################################

import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
import collections
import random
from sklearn.ensemble import GradientBoostingClassifier
from sklearn import metrics
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve, auc
import joblib
from joblib import parallel_backend
import math
# from Modules.dedupe_check import dedupe_checker
from Modules.riskmodel import predictRisk
from tabulate import tabulate
import warnings
from __init__ import *

warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)

path = 'dependencies/'
pickleloc = "pickle/"
save_path = "Output/"

date_today = pd.to_datetime(date.today(), infer_datetime_format=True, errors='coerce')
rundate = str(date_today)[8:10]+str(date_today)[5:7]+str(date_today)[2:4]



###########################################################################################################################################
CIBIL_T = 700
min_emi_T = 3000
min_amt_T = 50000
###########################################################################################################################################


def score_bkt(row, var):
    if row[var] <= 600:
        return '<=600'
    elif row[var] <= 620:
        return '601-620'
    elif row[var] <= 640:
        return '621-640'
    elif row[var] <= 660:
        return '641-660'
    elif row[var] <= 680:
        return '661-680'
    elif row[var] <= 700:
        return '681-700'
    elif row[var] <= 720:
        return '701-720'
    elif row[var] <= 740:
        return '721-740'
    elif row[var] <= 760:
        return '741-760'
    elif row[var] <= 780:
        return '761-780'
    elif row[var] <= 800:
        return '781-800'
    else:
        return '800+'


def line_bkt(row):
    if row['BC28S'] <= 0:
        return '<=0'
    elif row['BC28S'] <= 25000:
        return '1-25000'
    elif row['BC28S'] <= 50000:
        return '25001-50000'
    elif row['BC28S'] <= 75000:
        return '50001-75000'
    elif row['BC28S'] <= 100000:
        return '75001-100000'
    elif row['BC28S'] <= 150000:
        return '100001-150000'
    elif row['BC28S'] <= 200000:
        return '150001-200000'
    elif row['BC28S'] <= 300000:
        return '200001-300000'
    elif row['BC28S'] <= 400000:
        return '300001-400000'
    elif row['BC28S'] <= 500000:
        return '500001-500000'
    else:
        return '500000+'


def tenor(row):
    if (row['riskband'] == 'CAT-A'):
        return 24
    elif (row['riskband'] == 'CAT-B'):
        return 24
    elif (row['riskband'] == 'CAT-C'):
        return 18
    elif (row['riskband'] == 'CAT-D'):
        return 12
    else:
        return 0


def freeIncomeMult(row):
    if (row['riskband'] == 'CAT-A') & ((row['riskband2'] == 'L') | (row['riskband2'] == 'M')):
        return 1
    elif (row['riskband'] == 'CAT-A') & (row['riskband2'] == 'H'):
        return 0.8
    elif (row['riskband'] == 'CAT-B') & ((row['riskband2'] == 'L') | (row['riskband2'] == 'M')):
        return 1
    elif (row['riskband'] == 'CAT-B') & (row['riskband2'] == 'H'):
        return 0.7
    elif (row['riskband'] == 'CAT-C') & ((row['riskband2'] == 'L') | (row['riskband2'] == 'M')):
        return 1
    elif (row['riskband'] == 'CAT-C') & (row['riskband2'] == 'H'):
        return 0.6
    elif (row['riskband'] == 'CAT-D') & (row['riskband2'] == 'L'):
        return 1
    elif (row['riskband'] == 'CAT-D') & (row['riskband2'] == 'M'):
        return 0.8
    elif (row['riskband'] == 'CAT-D') & (row['riskband2'] == 'H'):
        return 0.7
    else:
        return 0

# ---THIS iS THE RULES ENGINE


def apply_bureau(row):
    # if row['pincode_Approved']==0:
    #    return 'pin do not match to niro-pin'
    if row['PAN_NO_Checked'] == 'wrong pan':
        return 'PAN Error'
    elif row['Calculated_Age'] < 21:
        return 'age<21'
    # elif row['Calculated_Age'] > 57:
    #     return 'age>57'
    elif row['gender'] == 'Transgender':
        return 'Transgender'
    elif row['CIBILTUSC3 Score Value'] < CIBIL_T:
        return 'CIBIL_LT_'+str(CIBIL_T)
    elif row['AT20S'] < 12:
        return 'Months on Bureau<12'
    elif row['CV11_24M']+row['CV12'] > 0:
        return '1+ Trade60+ in 2Y'
    elif row['G310S_6M'] > 1.5:
        return '1+ Trade30+ in 6M'
    elif row['G310S_3M'] > 1:
        return '1+ Trade 1DPD+ in 3M'
    # new rule on G310S UPDATED on 19-04-2023
    elif row['G310S'] >= 2:
        return '1+ Trade 60+ ever'
    #----------------------
    elif row['CO01S180'] > 0:
        return 'Past Write-off'
    elif row['CV14_6M'] >= 8:
        return '10+ Inq in lst 6Mth'
    elif row['AT09S_3M'] >= 2:
        return '2+ Trades opened in last 3Month'
    elif (row['AGG911'] > 95) & (row['BCPMTSTR'] in ['REVOLVER', 'RVLRPLUS']):
        return '90+% Bank Card Utilization in last 12 month'
    elif (row['AGGS911'] > 90) & (row['BCPMTSTR'] in ['REVOLVER', 'RVLRPLUS']):
        return '90+% Bank Card Utilization'
    # elif row['TRD'] <= 1 and row['riskband'] == 'CAT-A':
    #     return 'catA and trade<=1'
    # elif row['TRD'] <= 1 and row['riskband'] == 'CAT-B':
    #     return 'catB and trade<=1'
    # elif row['TRD'] <= 2 and row['riskband'] == 'CAT-C':
    #     return 'cat C and trade<=2'
    # elif row['TRD'] <= 2 and row['riskband'] == 'CAT-D':
    #     return 'cat D and trade<=2'
    # elif row['riskband2'] == 'H':
    #     return 'High Risk'
    elif row['Current_Salary'] < 26000:
        return 'Income<25K'
    elif row['Current_Salary']-row['Total_EMI_due']-13000 < 3000:
        return '1.No Disposable Income'
    elif (row['Total_EMI_due']+min_emi_T)/row['Current_Salary'] > 0.7:
        return 'FOIR>70%'
    elif row['available_income'] < 3000:
        return '2.No Disposable Income'
    elif ((row['available_income'] < 4000) & (row['Current_Salary'] < 30000) &
          (row['CIBILTUSC3 Score Value'] < CIBIL_T+40)):
        return '1.High Risk Account'
    elif ((row['available_income'] < 4000) & (row['Current_Salary'] < 30000) &
          (row['TRD'] <= 3) & (row['CV14_6M'] >= 6)):
        return '2.High Risk Account'
    # updated on 17/04/2023
    #----------------------------------
    elif (row['Current_Salary'] > 26000) & (row['BCPMTSTR'] == 'NOBC') & (row['CV14_12M']>=8):
        return "NOBC with 8+ Inq in 12M"
    elif (row['Current_Salary'] > 26000) & (row['BCPMTSTR'] == 'NOBC') & (row['CV14_12M']<8) & (row['CIBILTUSC3 Score Value']<732) & (row['CV14_6M']>3):
        return "NOBC with 3+ Inq in 6M"
    elif (row['Current_Salary'] > 26000) & (row['BCPMTSTR'] == 'NOBC') & (row['CV14_12M']<8) & (row['CIBILTUSC3 Score Value']>732) & (row['AT20S']<=32) & (row['AT09S_12M']>2) & (row['CIBILTUSC3 Score Value']<751):
        return "NOBC with Cibil LT 750"
    elif (row['Unsecured High Credit Sum']<=82000) & (row['CV14_3M']>=2) & (row['AT01S']>=3):
        return "High Credit Sum with 2+ Inq in 3M"
    #-----------------------------------
    elif row['riskband2'] == 'JUNK':
        return "naps_below_thresholds"
    else:
        return 'NOT DECLINED'


def freeIncomeMult(row):
    if (row['riskband'] == 'CAT-A') & ((row['riskband2'] == 'L') | (row['riskband2'] == 'M')):
        return 1
    elif (row['riskband'] == 'CAT-A') & (row['riskband2'] == 'H'):
        return 0.8
    elif (row['riskband'] == 'CAT-B') & ((row['riskband2'] == 'L') | (row['riskband2'] == 'M')):
        return 1
    elif (row['riskband'] == 'CAT-B') & (row['riskband2'] == 'H'):
        return 0.7
    elif (row['riskband'] == 'CAT-C') & ((row['riskband2'] == 'L') | (row['riskband2'] == 'M')):
        return 1
    elif (row['riskband'] == 'CAT-C') & (row['riskband2'] == 'H'):
        return 0.6
    elif (row['riskband'] == 'CAT-D') & (row['riskband2'] == 'L'):
        return 1
    elif (row['riskband'] == 'CAT-D') & (row['riskband2'] == 'M'):
        return 0.8
    elif (row['riskband'] == 'CAT-D') & (row['riskband2'] == 'H'):
        return 0.7
    else:
        return 0


def testControl2(row):
    return 'Control'


################################################################################################################
def rule_engine(dfCV):
    dfCV.rename(columns={"pred_propensity":"PROPENPLSC Score Value"},inplace=True)
    logger.info("Length of cibil_data in rule engine="+str(len(dfCV)))

    ############################CV Based Scoring
    
    dfCV['transactor'] = np.select(condlist=[dfCV['BCPMTSTR']=='TRANPLUS',dfCV['BCPMTSTR']=='TRANSACTOR'],choicelist=[2,1],default=0)
    dfCV['cc_other'] = np.select(condlist=[dfCV['BCPMTSTR']=='INDTRM',dfCV['BCPMTSTR']=='INACTIVE'],choicelist=[2,1],default=0)
    dfCV['cardlim'] = np.select(condlist=[dfCV['BC28S']>=500000,
                                   dfCV['BC28S']>=300000,
                                   dfCV['BC28S']>=150000,
                                   dfCV['BC28S']>=100000,
                                   dfCV['BC28S']>50000],
                         choicelist=[6,4,2,1,0],
                         default=-1)
    
    dfCV['open_auto_sanctions'] = dfCV['AU28S']
    dfCV['open_auto_sanctions'][dfCV['open_auto_sanctions']<=0] = 0
    dfCV['open_auto_sanctions'].fillna(0,inplace=True)

    dfCV['open_mortgage_sanctions'] = dfCV['MT28S']
    dfCV['open_mortgage_sanctions'][dfCV['open_mortgage_sanctions']<=0] = 0
    dfCV['open_mortgage_sanctions'].fillna(0,inplace=True)

    dfCV['open_personal_sanctions'] = dfCV['PL28S']
    dfCV['open_personal_sanctions'][dfCV['open_personal_sanctions']<=0] = 0
    dfCV['open_personal_sanctions'].fillna(0,inplace=True)

    dfCV['open_revolving_sanctions'] = dfCV['BC28S']
    dfCV['open_revolving_sanctions'][dfCV['open_revolving_sanctions']<=0] = 0
    dfCV['open_revolving_sanctions'].fillna(0,inplace=True)


    df2 = []

    #######################END CV Based Scoring

    dfnaps = dfCV.copy()

    #dfnaps.rename(columns={'pred_salary':'Predicted_Salary'},inplace=True)

    dfnaps['delq_ind'] = (dfnaps['CV10']*2)+(dfnaps['CV11']*3)+(dfnaps['CV12']*4)+\
                         (dfnaps['G310S_3M']*1)
    dfnaps['foir_dlq'] = np.select(condlist=[dfnaps['delq_ind']>=4,\
                                         dfnaps['delq_ind']>=3,\
                                         dfnaps['delq_ind']>=2,\
                                         dfnaps['delq_ind']>=1,],
                                   choicelist=[1.5,1,0.6,0.5],
                                   default = 0.4)
    dfnaps['foir_cibil'] = np.select(condlist=[dfnaps['CIBILTUSC3 Score Value']<=10,\
                                               dfnaps['CIBILTUSC3 Score Value']<680],
                                     choicelist=[0.5,\
                                             (62.5-0.082*dfnaps['CIBILTUSC3 Score Value']).astype(int)*0.1],
                                     default=(18.4-0.018*dfnaps['CIBILTUSC3 Score Value']).astype(int)*0.1)

    dfnaps['foir_trd'] = np.select(condlist=[((dfnaps['MT28S']>1000000)&(dfnaps['AU28S']>600000)),\
                                         (dfnaps['MT28S']>1000000),\
                                         (dfnaps['AU28S']>600000),\
                                         (dfnaps['MT28S']>200000),\
                                         (dfnaps['AU28S']>300000),\
                                         (dfnaps['BC28S']>100000),\
                                         (dfnaps['BC28S']>20000),],
                                   choicelist=[0.35,0.36,0.37,0.39,0.4,0.43,0.45],
                                   default=0.5)

    dfnaps['foir_mean'] = (dfnaps['foir_dlq'] + dfnaps['foir_cibil'] + dfnaps['foir_trd'])/3

    dfnaps['auto_sanction_open'] = dfnaps['AU28S']*(dfnaps['AU33S']>10000)
    dfnaps['mrtg_sanction_open'] = dfnaps['MT28S']*(dfnaps['MT33S']>20000)
    dfnaps['secu_sanction_open'] = np.select(condlist=[(dfnaps['Secured Balances Sum']-\
                                                       (dfnaps['AU33S']*(dfnaps['AU33S']>0))-\
                                                       (dfnaps['MT33S']*(dfnaps['MT33S']>0)))>0],
                                             choicelist=[dfnaps['Secured Balances Sum']-\
                                                        (dfnaps['AU33S']*(dfnaps['AU33S']>0))-\
                                                        (dfnaps['MT33S']*(dfnaps['MT33S']>0))],
                                             default = 0)
    
        
    dfnaps['Categories_Count'] = np.select (condlist=[((dfnaps['CIBILTUSC3 Score Value'] >= 780) &\
                                                        (dfnaps['BC28S'] >= 200000) & \
                                                      (dfnaps['AT20S'] >= 36)&(dfnaps['TRD']>=3)) ,
                                                      ((dfnaps['CIBILTUSC3 Score Value'] >= 740) &\
                                                      ((dfnaps['BC28S'] >= 100000) | (dfnaps['BC28S']+dfnaps['AU28S']+dfnaps['MT28S']+dfnaps['PL28S'] >= 250000)) & \
                                                          (dfnaps['AT20S'] >= 36)&(dfnaps['TRD']>=3)),
                
                                                          ((dfnaps['BC28S'] >= 50000) | (dfnaps['BC28S']+dfnaps['AU28S']+dfnaps['MT28S']+dfnaps['PL28S'] >= 100000)) & (dfnaps['CIBILTUSC3 Score Value'] >= 720)],
                                            choicelist = ['1.CAT-A','2.CAT-B','3.CAT-C'],
                                            default = 0)
        
    dfnaps['pl_sanction_open'] = dfnaps['PL28S']*(dfnaps['PL33S']>10000)
    dfnaps['cc_sanction_open'] = dfnaps['BC28S']

    dfnaps['Total_EMI_due']= (dfnaps['auto_sanction_open'].clip(0,None)*0.02)+\
                             (dfnaps['mrtg_sanction_open'].clip(0,None)*0.008)+\
                             (dfnaps['secu_sanction_open'].clip(0,None)*0.01)+\
                             (dfnaps['Unsecured Balances Sum'].clip(0,None)*0.05)

    dfnaps['Expected_Salary'] = dfnaps['Total_EMI_due']/dfnaps['foir_mean']

    dfnaps['Expected_Salary'] = np.select(condlist=[((dfnaps['Expected_Salary']<22000)&(dfnaps['cc_sanction_open']>=10000)\
                                                     &(dfnaps['cc_sanction_open']<=66000)),\
                                                    ((dfnaps['Expected_Salary']<150000)&(dfnaps['cc_sanction_open']>450000)),\
                                                    ((dfnaps['Expected_Salary']<0.3*dfnaps['cc_sanction_open'])\
                                                     &(dfnaps['cc_sanction_open']>=60000)),\
                                                    dfnaps['Expected_Salary']<7500],
                                          choicelist=[23000,150000,0.35*dfnaps['cc_sanction_open'],\
                                                      dfnaps['pred_salary']*0.8],
                                          default=dfnaps['Expected_Salary'])

    dfnaps['auto_surrogate_sal'] = np.select(condlist=[dfnaps['AU28S']<=200000,dfnaps['AU28S']<=500000,\
                                                       dfnaps['AU28S']<=1000000,dfnaps['AU28S']>1000000],\
                                             choicelist=[20000,dfnaps['AU28S']*0.08,dfnaps['AU28S']*0.11,\
                                                         dfnaps['AU28S']*0.14],
                                             default=[33000])
    dfnaps['mrtg_surrogate_sal'] = np.select(condlist=[dfnaps['MT28S']<=200000,dfnaps['MT28S']<=1000000,\
                                                       dfnaps['MT28S']<=7500000,dfnaps['MT28S']>7500000],\
                                             choicelist=[40000,dfnaps['MT28S']*0.05,dfnaps['MT28S']*0.04,\
                                                       dfnaps['MT28S']*0.038],
                                             default=[33000])
    dfnaps['prsn_surrogate_sal'] = np.select(condlist=[dfnaps['PL28S']<=25000,dfnaps['PL28S']<=100000,\
                                                       dfnaps['PL28S']<=200000,dfnaps['PL28S']<=350000,\
                                                       dfnaps['PL28S']>350000],
                                             choicelist=[12000,23000,28000,32000,dfnaps['PL28S']*0.125],
                                             default=[33000])
    dfnaps['card_surrogate_sal'] = dfnaps['BC28S']*0.3
    dfnaps['minsal'] = 15000
    dfnaps['maxsal'] = 500000
    dfnaps['Current_Salary'] = dfnaps[['auto_surrogate_sal','mrtg_surrogate_sal',\
                                       'prsn_surrogate_sal','card_surrogate_sal',\
                                       'Expected_Salary','minsal']].max(axis=1)
    dfnaps['Current_Salary'] = dfnaps[['Current_Salary','maxsal']].min(axis=1)
    dfnaps[["riskband", "riskband2", "naps_tu", "naps_tu_thirtyplus"]] = predictRisk(dfnaps)

    `maxcatloan_map = collections.defaultdict(lambda:0,{'CAT-A':500000,'CAT-B':400000,'CAT-C':180000,'CAT-D':62000})``
    maxcat2loan_map = collections.defaultdict(lambda:0,{'L':500000,'M':400000,'H':70000})
    # rate_map = collections.defaultdict(lambda:28,{'CAT-A':18.9,'CAT-B':22.9,'CAT-C':24.9,'CAT-D':28.9})
    rate_map = defaultdict(
    lambda: 28,
    {
        "CAT-A": collections.defaultdict(lambda: 28, {"L": 18.9, "M": 18.9, "H": 18.9}),
        "CAT-B": collections.defaultdict(lambda: 28, {"L": 20.9, "M": 21.9, "H": 22.9}),
        "CAT-C": collections.defaultdict(lambda: 28, {"L": 23.9, "M": 23.9, "H": 23.9}),
        "CAT-D": collections.defaultdict(lambda: 28, {"L": 27.9, "M": 27.9, "H": 27.9}),
    },
    )
    ratemult_map = collections.defaultdict(lambda:0,{'H':1,'M':0,'L':0})
    ####################landing partner decision for Muthoot(31%)-2Dec2022
    # j=len(dfnaps[dfnaps['CIBILTUSC3 Score Value']>=740])
    # logger.info("count of records with 740+ score :",j)
    # dfnaps['lending partner']='Unassigned'
    # df_Muthoot_740_plus=dfnaps[((dfnaps['Muthoot_serviceable']=="yes")&(dfnaps['CIBILTUSC3 Score Value']>=740))] #df_Muthoot2
    # logger.info("count of records with score >=740 that can be given to Muthoot=",len(df_Muthoot_740_plus))
    # n=int(len(dfnaps[dfnaps['CIBILTUSC3 Score Value']>=740])*.5) #.31
    # muthoot2 = df_Muthoot_740_plus.sample(n,replace=False,random_state=np.random.RandomState()).index
    # dfnaps.loc[muthoot2,'lending partner'] = "Muthoot"
    

    # logger.info("Unassigned records :",len(dfnaps[dfnaps['lending partner']=="Unassigned"]))
    # logger.info("Muthoot records with score >=740 :",len(dfnaps[dfnaps['lending partner']!="Unassigned"]))


    
    tenor_map = collections.defaultdict(lambda:0,{'CAT-A':36,'CAT-B':36,'CAT-C':18,'CAT-D':12})
    tenorMax_map = collections.defaultdict(lambda:0,{'CAT-A':36,'CAT-B':36,'CAT-C':24,'CAT-D':18})
    #tenor updataed for muthoot on 8nov2022
    # tenor_map_Unassigned = collections.defaultdict(lambda:0,{'CAT-A':36,'CAT-B':36,'CAT-C':18,'CAT-D':12})
    # tenorMax_map_Unassigned = collections.defaultdict(lambda:0,{'CAT-A':36,'CAT-B':36,'CAT-C':24,'CAT-D':18})
    # tenor_map_Muthoot = collections.defaultdict(lambda:0,{'CAT-A':60,'CAT-B':48,'CAT-C':18,'CAT-D':12})
    # tenorMax_map_Muthoot = collections.defaultdict(lambda:0,{'CAT-A':60,'CAT-B':48,'CAT-C':24,'CAT-D':18})
        
    
    pf_map = reg_pf_map if DATA_PARTNER!='Finbud' else fin_pf_map

    dfnaps['mean_cclim'] = np.select(condlist=[dfnaps['BC28S']>0,
                                               dfnaps['BC28S']<0,],
                                     choicelist=[dfnaps['BC28S']/dfnaps['BC02S'], 0],
                                     default = 0)

    dfnaps['mean_securedLim'] = np.select(condlist=[dfnaps['Secured High Credit Sum']>0,
                                                    dfnaps['Secured High Credit Sum']<0,],
                                          choicelist=[dfnaps['Secured High Credit Sum']/dfnaps['Secured Accounts Count'],0],
                                          default = 0)
    dfnaps['mean_unsecuredLim'] = np.select(condlist=[dfnaps['Unsecured High Credit Sum']>0,
                                                      dfnaps['Unsecured High Credit Sum']<0,],
                                            choicelist=[dfnaps['Unsecured High Credit Sum']/dfnaps['Unsecured Accounts Count'],0],
                                            default = 0)

    dfnaps['CC_lineband']= dfnaps.apply(line_bkt,axis=1)
    dfnaps['naps_bkt']= dfnaps.apply(score_bkt,var='naps_tu',axis=1)
    dfnaps['cibilv3_bkt']= dfnaps.apply(score_bkt,var='CIBILTUSC3 Score Value',axis=1)
    dfnaps['available_income']= (dfnaps['Current_Salary']*0.7)-dfnaps['Total_EMI_due']

    rateTestMap = collections.defaultdict(lambda:0,{'Control':0,\
                                                    'Low_Rate_Test1':-2,\
                                                    'Low_Rate_Test2':-2,\
                                                    'HiAmt_LoBrdn_LoRate_Test2': -6})

    tenorBurdenTestMap = collections.defaultdict(lambda:0,{'Control':0,\
                                                          'Low_Burden_Test1':3,\
                                                          'Low_Burden_Test2':6,\
                                                          'HiAmt_LoBrdn_LoRate_Test2': 6})

    amountTestMap = collections.defaultdict(lambda:1,{'Control':1,\
                                                      'High_Amount_Test1':1.3,\
                                                      'HiAmt_LoBrdn_LoRate_Test2':1.3})

    random.seed(43)

    dfnaps['dec_reason']= dfnaps.apply(apply_bureau,axis=1)

    dfnaps['testControlTag']=dfnaps.apply(testControl2,axis=1)

    dfnaps['max_tenor']=dfnaps['riskband'].map(tenor_map)  
    
    # updated below on 8 nov 2022
                              
    # dfnaps['max_tenor']= np.select(condlist=[dfnaps['lending partner']=="Unassigned"],
    #                                choicelist=[dfnaps['riskband'].map(tenor_map_Unassigned)],
    #                                default=dfnaps['riskband'].map(tenor_map_Muthoot))
    

    ########################THIS IS THE OFFER GENERATION METHOD
    dfnaps['freeIncomeMult']= dfnaps.apply(freeIncomeMult,axis=1)

    #Original LA based on benchark tenor
    dfnaps['max_loan_possible'] = (((dfnaps['available_income']*\
                                     dfnaps['max_tenor']*\
                                     dfnaps['freeIncomeMult'])/1000).astype(int))*1000

    dfnaps['max_cat_loan'] = dfnaps['riskband'].map(maxcatloan_map)
    dfnaps['max_cat2_loan'] = dfnaps['riskband2'].map(maxcat2loan_map)

    #modifying max salary based loan based on TC flag
    dfnaps['max_sal_loan'] = np.select(condlist=[dfnaps['Current_Salary']<25000, 
                                                 dfnaps['Current_Salary']<33000],
                                       choicelist=[75000,100000],
                                       default=dfnaps['max_loan_possible'])

    #Modifying Offer for TC FLag
    dfnaps['SYSTEM_LOAN_OFFER'] = dfnaps[['max_loan_possible','max_cat_loan',\
                                          'max_cat2_loan','max_sal_loan']].min(axis=1)*\
                                          dfnaps['testControlTag'].map(amountTestMap)

    dfnaps['dec_reason'] = np.select(condlist=[dfnaps['SYSTEM_LOAN_OFFER']<=0,
                                               ((dfnaps['dec_reason']=="NOT DECLINED")&\
                                               (dfnaps['SYSTEM_LOAN_OFFER']< min_amt_T))],
                                     choicelist=[dfnaps['dec_reason'],'No Disposable Income'],
                                     default = dfnaps['dec_reason'])

    dfnaps['SYSTEM_LOAN_OFFER'] = np.select(condlist=[dfnaps['SYSTEM_LOAN_OFFER']<=0,
                                                      dfnaps['SYSTEM_LOAN_OFFER']<min_amt_T],
                                            choicelist=[0,0],
                                            default = dfnaps['SYSTEM_LOAN_OFFER'])
    #Reset the Max Tenor based on TC FLag now
    dfnaps['max_tenor']= dfnaps['max_tenor'] +\
                        dfnaps['testControlTag'].map(tenorBurdenTestMap)
    dfnaps['gmax_Ten'] = dfnaps['riskband'].map(tenorMax_map)
    #code changed on 8Nov 2022 as below
    # dfnaps['gmax_Ten'] = np.select(condlist=[dfnaps['lending partner']=="Unassigned"],
    #                                choicelist=[dfnaps['riskband'].map(tenorMax_map_Unassigned)],
    #                                default=dfnaps['riskband'].map(tenorMax_map_Muthoot))
    
    dfnaps['new_tenor'] = dfnaps[['max_tenor','gmax_Ten']].min(axis=1)
    dfnaps.drop(columns={'max_tenor','gmax_Ten'},inplace=True)
    dfnaps.rename(columns={'new_tenor':'max_tenor'},inplace=True)
    dfnaps["rate_temp"] = list(map(lambda x, y: rate_map[x][y], dfnaps["riskband"], dfnaps["riskband2"]))
    dfnaps['SYSTEM_RATEOFINT'] = dfnaps['rate_temp'] + \
                                 dfnaps['riskband2'].map(ratemult_map) +\
                                 dfnaps['testControlTag'].map(rateTestMap)
    dfnaps['SYSTEM_PF'] = dfnaps['riskband'].map(pf_map)
    dfnaps['GST_Perc'] = 18

    dfnaps['roi_m'] = dfnaps['SYSTEM_RATEOFINT']/1200
    dfnaps['amort'] = (1 +  dfnaps['roi_m'])**dfnaps['max_tenor']

    dfnaps['SYSTEM_MAX_EMI'] = ((dfnaps['SYSTEM_LOAN_OFFER']*\
                               (dfnaps['roi_m']*dfnaps['amort'])/(dfnaps['amort']-1))/100).fillna(0).astype(int)*100

    dfnaps['New_FOIR'] = ((dfnaps['SYSTEM_MAX_EMI']+dfnaps['Total_EMI_due']+10000)*100/dfnaps['Current_Salary']).fillna(0)

    dfnaps['SYSTEM_MAX_EMI'] = np.select(condlist=[dfnaps['New_FOIR']>=65],
                                         choicelist=[(0.65*dfnaps['Current_Salary'])-dfnaps['Total_EMI_due']-10000],
                                         default = dfnaps['SYSTEM_MAX_EMI'])

    dfnaps['dec_reason'] = np.select(condlist=[((dfnaps['dec_reason']=="NOT DECLINED")&\
                                                (dfnaps['SYSTEM_MAX_EMI']>0)&\
                                                (dfnaps['SYSTEM_MAX_EMI']<= min_emi_T))],
                                     choicelist=['FOIR>70% with New EMI'],
                                     default = dfnaps['dec_reason'])

    dfnaps['l2'] = dfnaps['SYSTEM_MAX_EMI']*(dfnaps['amort']-1)/(dfnaps['roi_m']*dfnaps['amort'])
    dfnaps['l3'] = dfnaps['l2']/(1+(dfnaps['SYSTEM_PF']/100)+((dfnaps['GST_Perc']/100)*(dfnaps['SYSTEM_PF']/100)))
    dfnaps['l4temp'] = 5000*((dfnaps['l3']/5000).astype(int))
    dfnaps['l4'] = np.select( condlist=[((dfnaps['l3']>=49000) & (dfnaps['l3']<51000))],
                                        choicelist=[51000],
                                        default=dfnaps['l4temp'])
    dfnaps['SYSTEM_LOAN_OFFER'] = dfnaps['l4']
    dfnaps['dec_reason'] = np.select(condlist=[((dfnaps['dec_reason']=="NOT DECLINED")&\
                                               (dfnaps['l4']< min_amt_T))],
                                     choicelist=['3.No Disposable Income'],
                                     default = dfnaps['dec_reason'])

    dfnaps['ImputedInc'] = dfnaps['Current_Salary'].clip(20000,400000)
    dfnaps['Current_Salary'] = np.select(condlist=[dfnaps['ImputedInc']<25000,dfnaps['ImputedInc']<=30000 ],
                                         choicelist=[dfnaps['ImputedInc']+15000, dfnaps['ImputedInc']+10000],
                                         default = dfnaps['ImputedInc']*1.2)

    dfnaps['dec_reason'] = np.select(condlist=[((dfnaps['dec_reason']=="NOT DECLINED")&(dfnaps['SYSTEM_LOAN_OFFER']< min_amt_T + 1000))],
                                     choicelist=['SYSTEM_LOAN_OFFER<'+str(min_amt_T + 1000)],
                                     default = dfnaps['dec_reason'])
    
    # dfnaps['dec_reason'] = np.select(condlist=[((dfnaps['dec_reason']=="NOT DECLINED")&(dfnaps['riskband']=="CAT-D")\
    #                                            &(dfnaps['riskband2']=="H")&(dfnaps['TRD']<2))],
    #                                  choicelist=['High Risk Account'],
    #                                  default = dfnaps['dec_reason'])
    #Added on 29 Nov 2022
    # dfnaps['dec_reason'] = np.select(condlist=[((dfnaps['riskband2']=="H")&(dfnaps['riskband']=="CAT-C")),
    #                                             ((dfnaps['riskband2']=="H")&(dfnaps['riskband']=="CAT-D"))],
    #                                   choicelist=['High Risk Account CAT-C','High Risk Account CAT-D'],
    #                                   default = dfnaps['dec_reason'])

    #############################################################################----lending partner decision for Liquiloan
    
    
    
    #######################################################################################################################
    logger.info("count of dfnaps['SYSTEM_LOAN_OFFER']>=250000 is :",len(dfnaps[dfnaps['SYSTEM_LOAN_OFFER']>=250000]))
    ########## Old Lending Partner

    dfnaps['lending partner']=np.select(condlist=[((dfnaps['Muthoot_serviceable']=="yes")&(dfnaps['PayU_serviceable']=="yes")) | ((dfnaps['PayU_serviceable']=="yes")&(dfnaps['Muthoot_serviceable']=="no")),
                                                 ((dfnaps['Muthoot_serviceable']=="yes")&(dfnaps['PayU_serviceable']=="no")&(dfnaps['SYSTEM_LOAN_OFFER']<=250000)&(dfnaps['riskband'].isin(muthoot_switchable_cats))),
                                                  ((dfnaps['PayU_serviceable']=="no")&(dfnaps['Muthoot_serviceable']=="no")&(dfnaps['LL_serviceable']=="no"))],
                                        choicelist=["PayU","PayU",'Liquiloans'],
                                        default='Liquiloans')
    # dfnaps['dec_reason'] = np.select(condlist=[((dfnaps['PayU_serviceable']=="no")&(dfnaps['Muthoot_serviceable']=="no")&(dfnaps['LL_serviceable']=="no"))],
    #                                  choicelist=['Pincode_not_serviceable'],
    #                                  default=dfnaps['dec_reason'])


    dfnaps['hashed_phone'] = ''
    # n740muthoot=int(len(dfnaps[dfnaps['lending partner']=='Muthoot']))
                                   
    # dfnaps['lending partner']=np.select(condlist=[((dfnaps['Muthoot_serviceable']=="yes")&(dfnaps['PayU_serviceable']=="yes")&(dfnaps['lending partner']=="Unassigned")),
    #                                              ((dfnaps['Muthoot_serviceable']=="yes")&(dfnaps['PayU_serviceable']=="no")&(dfnaps['lending partner']=="Unassigned")),
    #                                               ((dfnaps['PayU_serviceable']=="yes")&(dfnaps['Muthoot_serviceable']=="no")&(dfnaps['lending partner']=="Unassigned")),
    #                                               ((dfnaps['PayU_serviceable']=="no")&(dfnaps['Muthoot_serviceable']=="no")&(dfnaps['pincode_Approved']==0)),
    #                                               (dfnaps['lending partner']=="Unassigned")],
    #                                     choicelist=["Muthoot-PayU","Muthoot","PayU","Muthoot-PayU","Liquiloans"],
    #                                     default=dfnaps['lending partner'])
    
    # #####################################################changing Muthoot-PayU 50-50 to Muthoot and PayU
    # if (n740muthoot<int(len(dfnaps[dfnaps['lending partner']=="Muthoot-PayU"])*.5)):
    #     n=n740muthoot+int(len(dfnaps[dfnaps['lending partner']=="Muthoot-PayU"])*.5)
    #     df_change=dfnaps[dfnaps['lending partner'] == 'Muthoot-PayU']
    #     change = df_change.sample(n,replace=False,random_state=np.random.RandomState()).index
    #     dfnaps.loc[change,'lending partner'] = "PayU"
    # else :
    #     n=(n740muthoot-(int(len(dfnaps[dfnaps['lending partner']=="Muthoot-PayU"])*.5)))+int(len(dfnaps[dfnaps['lending partner']=="Muthoot-PayU"])*.5)
    #     df_change=dfnaps[dfnaps['lending partner'] == 'Muthoot-PayU']
    #     change = df_change.sample(n,replace=False,random_state=np.random.RandomState()).index
    #     dfnaps.loc[change,'lending partner'] = "PayU"
    # ##############50% given to PayU,Rest Muthoot-PayU is being given to PayU
    # logger.info("left Muthoot-PayU count",len(dfnaps[dfnaps['lending partner']=="Muthoot-PayU"]))
    # dfnaps['lending partner']=np.select(condlist=[dfnaps['lending partner']=="Muthoot-PayU"],
    #                                     choicelist=["PayU"],
                                        # default=dfnaps['lending partner'])


    ###########################################################################################################################
    ###########################################################################################################################
    """change made-12Nov2022
    dfnaps['SYSTEM_LOAN_OFFER']=np.select( condlist=[dfnaps['lending partner']=='Liquiloans'],      
                                           choicelist=[240000],
                                           default=dfnaps['SYSTEM_LOAN_OFFER'])
    dfnaps['max_tenor']=np.select( condlist=[dfnaps['lending partner']=='Liquiloans'],      
                                   choicelist=[24],
                                   default=dfnaps['max_tenor'])
    """
    dfnaps['SYSTEM_LOAN_OFFER']=np.select( condlist=[(dfnaps['lending partner']=='Liquiloans')&(dfnaps['SYSTEM_LOAN_OFFER']>240000)],      
                                           choicelist=[(240000)],
                                           default=dfnaps['SYSTEM_LOAN_OFFER'])
    dfnaps['max_tenor']=np.select( condlist=[(dfnaps['lending partner']=='Liquiloans')&(dfnaps['max_tenor']>24)],      
                                   choicelist=[24],
                                   default=dfnaps['max_tenor'])
    
    dfnaps['SYSTEM_MAX_EMI']=np.select( condlist=[dfnaps['lending partner']=='Liquiloans'],      
                                        choicelist=[(((dfnaps['SYSTEM_LOAN_OFFER']/dfnaps['max_tenor'])/100).fillna(0).astype(int))*100],
                                        default=dfnaps['SYSTEM_MAX_EMI'])
    
    dfnaps['roi_m'] = dfnaps['SYSTEM_RATEOFINT']/1200
    dfnaps['amort'] = (1 +  dfnaps['roi_m'])**dfnaps['max_tenor']
    dfnaps['l2'] = dfnaps['SYSTEM_MAX_EMI']*(dfnaps['amort']-1)/(dfnaps['roi_m']*dfnaps['amort'])
    dfnaps['l3'] = dfnaps['l2']/(1+(dfnaps['SYSTEM_PF']/100)+((dfnaps['GST_Perc']/100)*(dfnaps['SYSTEM_PF']/100)))
    dfnaps['l4temp'] = 5000*((dfnaps['l3']/5000).astype(int))
    dfnaps['l4'] = np.select( condlist=[((dfnaps['l3']>=49000) & (dfnaps['l3']<51000))],
                                        choicelist=[51000],
                                        default=dfnaps['l4temp'])
    dfnaps['SYSTEM_LOAN_OFFER'] = dfnaps['l4']
    dfnaps['dec_reason'] = np.select(condlist=[((dfnaps['dec_reason']=="NOT DECLINED")&(dfnaps['SYSTEM_LOAN_OFFER']< min_amt_T + 1000))],
                                     choicelist=['SYSTEM_LOAN_OFFER<'+str(min_amt_T + 1000)],
                                     default = dfnaps['dec_reason'])
    

    ################################################################################################
    dfnaps['data_source'] = DATA_SOURCE
    dfnaps['offer_source'] = OFFER_SOURCE
    logger.info('Length of all passed the rule engine before dedupe=', len(dfnaps[dfnaps['dec_reason'] == 'NOT DECLINED']))
    dfnaps['randNumCol'] = np.random.randint(1, 9999, dfnaps.shape[0])
    dfnaps['Platform_Partner_Name'] = PLATFORM_PARTNER
    #dfnaps['Data_Tag'] = DATA_PARTNER+'_'+rundate+'_'+dfnaps['DateofBirth'].astype("int64").astype(str)+'_'+dfnaps['randNumCol'].astype(str)
    #updated on 29.05.2023 applicable from offer number 280(PL)
    dfnaps['Data_Tag']= Data_Tag+DD
    dfnaps['Member Reference'] = dfnaps['Member Reference'].astype(float).astype('int64').astype(str).str[-10:]
    dfnaps.to_csv(save_path+OFFER_SOURCE+'.csv', index=False)



    return dfnaps
