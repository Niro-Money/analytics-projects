import pandas as pd
import numpy as np
import datetime as datetime
from dateutil.relativedelta import relativedelta

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

fileloc = "F:/Niro/Performance/Nov'23 Portfolio Performance/"
commloc = "F:/Niro/Performance/commonfiles/"
perf = pd.read_csv(fileloc+"Loan Book PL dec23.csv",low_memory=False)
perf = perf[~perf['niro_user_id'].isna()]
print("Len of performance file is: "+str(len(perf)))

def convert(varOut, VarIn):
    try:
        perf[varOut] = pd.to_numeric(perf[VarIn].str.replace(",","").str.replace("-","").str.strip())
    except:
        perf[varOut] = perf[VarIn]+1-1

convert('Principal','Loan Amount (all inclusive)')
convert('Prin0','Feb - POS')
convert('Prin1','Mar - POS')
convert('Prin2','Apr - POS')
convert('Prin3','May - POS')
convert('Prin4','Jun - POS')
convert('Prin5','Jul - POS')
convert('Prin6','Aug - POS')
convert('Prin7','Sep - POS')
convert('Prin8','Oct - POS')
convert('Prin9','Nov - POS')
convert('Prin10','Dec - POS')
convert('Prin11',"Jan'23 - POS")
convert('Prin12',"Feb'23 - POS")
convert('Prin13',"Mar'23 - POS")
convert('Prin14',"Apr'23 - POS")
convert('Prin15',"May'23 - POS")
convert('Prin16',"Jun'23 - POS")
convert('Prin17',"July'23 - POS")
convert('Prin18',"Aug'23 - POS")
convert('Prin19',"Sep'23 - POS")
convert('Prin20',"Oct'23 - POS")
convert('Prin21',"Nov'23 - POS")

perf['DPD0'] = perf['Feb DPD']
perf['DPD1'] = perf['Mar DPD']
perf['DPD2'] = perf['Apr DPD']
perf['DPD3'] = perf['May DPD']
perf['DPD4'] = perf['Jun DPD']
perf['DPD5'] = perf['Jul DPD']
perf['DPD6'] = perf['Aug DPD']
perf['DPD7'] = perf['Sep DPD']
perf['DPD8'] = perf['Oct DPD']
perf['DPD9'] = perf['Nov DPD']
perf['DPD10'] = perf['Dec DPD']
perf['DPD11'] = perf["Jan'23 DPD"]
perf['DPD12'] = perf["Feb'23 DPD"]
perf['DPD13'] = perf["Mar'23 DPD"]
perf['DPD14'] = perf["Apr'23 DPD"]
perf['DPD15'] = perf["May'23 DPD"]
perf['DPD16'] = perf["Jun'23 DPD"]
perf['DPD17'] = perf["July'23 DPD"]
perf['DPD18'] = perf["Aug'23 DPD"]
perf['DPD19'] = perf["Sep'23 DPD"]
perf['DPD20'] = perf["Oct'23 DPD"]
perf['DPD21'] = perf["Nov'23 DPD"]

perf['Presentation0'] = perf['Feb Presentation']
perf['Presentation1'] = perf['Mar Presentation']
perf['Presentation2'] = perf['Apr Presentation']
perf['Presentation3'] = perf['May Presentation']
perf['Presentation4'] = perf['Jun Presentation']
perf['Presentation5'] = perf['Jul Presentation']
perf['Presentation6'] = perf['Aug Presentation']
perf['Presentation7'] = perf['Sep Presentation']
perf['Presentation8'] = perf['Oct Presentation']
perf['Presentation9'] = perf['Nov Presentation']
perf['Presentation10'] = perf['Dec Presentation']
perf['Presentation11'] = perf["Jan'23 Presentation"]
perf['Presentation12'] = perf["Feb'23 Presentation"]
perf['Presentation13'] = perf["Mar'23 Presentation"]
perf['Presentation14'] = perf["Apr'23 Presentation"]
perf['Presentation15'] = perf["May'23 Presentation"]
perf['Presentation16'] = perf["Jun'23 Presentation"]
perf['Presentation17'] = perf["July'23 Presentation"]
perf['Presentation18'] = perf["Aug'23 Presentation"]
perf['Presentation19'] = perf["Sep'23 Presentation"]
perf['Presentation20'] = perf["Oct'23 Presentation"]
perf['Presentation21'] = perf["Nov'23 Presentation"]
perf['Presentation22'] = perf["Dec'23 Presentation"]


for x in range(21):
    p=x+1
    print(p)
    perf['Prin'+str(p)] = np.select(condlist=[(perf['Prin'+str(p)]>0).fillna(False),
                                              perf['Presentation'+str(p)].str.upper().str.contains('CLOSE').fillna(False),\
                                              perf['Presentation'+str(p+1)].str.upper().str.contains('CLOSE').fillna(False)],
                                    choicelist=[perf['Prin'+str(p)],0,0],
                                    default = perf['Prin'+str(p-1)])
    

perf['disb_dt'] = pd.to_datetime(perf['Disbursement Date'].str.upper(), format= '%d-%b-%y', errors = 'coerce')
perf['disb_yymm'] = pd.to_datetime('01'+perf['Disbursement Date'].str[2:], format= '%d-%b-%y', errors = 'coerce')
perf['foreclosure_dt'] = pd.to_datetime(perf['Foreclosure Date'].str.upper(), format= '%d-%b-%y', errors = 'coerce')
perf['mth_to_foreclose'] = 12*(perf['foreclosure_dt'].dt.year - perf['disb_yymm'].dt.year)+\
                           (perf['foreclosure_dt'].dt.month - perf['disb_yymm'].dt.month)

perf_enddate = datetime.datetime(2023,11,1)
perf_startdate = datetime.datetime(2022,2,1)
perf['months_since_disb'] = (12*(perf_enddate.year - perf['disb_yymm'].dt.year)+\
                           (perf_enddate.month - perf['disb_yymm'].dt.month)+1).astype(int)
perf['PF'] = pd.to_numeric(perf["PF %"].str.replace(" ","").str.replace("%","").str.strip())

recovery = pd.read_csv(fileloc+'recovery.csv',low_memory=False)
recovery['Niro Opportunity ID'] = recovery['Opportunity ID']
recovery['recovery'] = pd.to_numeric(recovery['POS at time of NPA'].str.replace(",",""),errors='coerce')-\
                       pd.to_numeric(recovery['Current POS'].str.replace(",",""),errors='coerce')
recovery = recovery[recovery['recovery']>0]
recovery = recovery[['Niro Opportunity ID','recovery']]

perf = perf.merge(recovery,how='left',on='Niro Opportunity ID')
perf['recovery'].fillna(0,inplace=True)

perf.to_csv(fileloc+'aa.csv')

####
attrs = pd.read_csv(fileloc+"dataInputs_dec.csv",low_memory=False)
attrs.fillna(method='ffill',inplace=True)
attrs['monthly_take_home_income'].fillna(25000,inplace=True)
attrs['ctc'] = pd.to_numeric(attrs['monthly_take_home_income'].str.replace(",","").str.replace("-","").fillna(25000),errors='coerce')
#income_source
#loan_amount

def cibil_score_bkt(row, var):
    if row[var] <= 736:
        return '<=736'
    elif row[var] <= 745:
        return '737-745'
    elif row[var] <= 749:
        return '746-749'
    elif row[var] <= 757:
        return '750-757'
    elif row[var] <= 760:
        return '758-760'
    elif row[var] <= 766:
        return '761-766'
    elif row[var] <= 772:
        return '767-772'
    elif row[var] <= 776:
        return '773-776'
    elif row[var] <= 784:
        return '777-784'
    else:
        return '785+'

def naps_score_bkt(row, var):
    if row[var] <= 689:
        return '<=689'
    elif row[var] <= 704:
        return '690-704'
    elif row[var] <= 713:
        return '705-713'
    elif row[var] <= 728:
        return '714-728'
    elif row[var] <= 742:
        return '723-742'
    elif row[var] <= 751:
        return '743-751'
    elif row[var] <= 770:
        return '752-770'
    elif row[var] <= 790:
        return '771-790'
    elif row[var] <= 818:
        return '791-818'
    else:
        return '819+'

def napstu_score_bkt(row, var):
    if row[var] <= 694:
        return '<=694'
    elif row[var] <= 705:
        return '695-705'
    elif row[var] <= 714:
        return '706-714'
    elif row[var] <= 727:
        return '715-727'
    elif row[var] <= 743:
        return '728-743'
    elif row[var] <= 759:
        return '744-759'
    elif row[var] <= 780:
        return '760-780'
    elif row[var] <= 805:
        return '781-805'
    elif row[var] <= 847:
        return '806-847'
    else:
        return '848+'


#statename
#BCPMTSTR

attrs['CIBIL'] = attrs.apply(cibil_score_bkt,var='CIBILTUSC3 Score Value',axis=1)

attrs['naps_band'] = attrs.apply(napstu_score_bkt,var='naps_tu',axis=1)

attrs['Credit cards'] = np.select(condlist=[attrs['BC02S']<=0,
                                             attrs['BC02S']<=2,
                                             attrs['BC02S']<=4,
                                             attrs['BC02S']>4,],choicelist=['No Credit Cards','1-2',
                                                                                        '3-4','5+'],default='No Credit Cards')
attrs['Years on Bureau'] = np.select(condlist=[attrs['AT20S']<=24,
                                             attrs['AT20S']<=48,
                                             attrs['AT20S']<=60,
                                             attrs['AT20S']>60,],choicelist=['1-2 Yrs','2-4 Yrs',
                                                                                        '4-5 Yrs','5+ Yrs'],default='4-5 Yrs')
attrs['Inq_12m'] = np.select(condlist=[attrs['CV14_12M']<=1,
                                             attrs['CV14_12M']<=3,
                                             attrs['CV14_12M']<=6,
                                             attrs['CV14_12M']>6,],choicelist=['<=1','2-3',
                                                                                        '4-6','7+'],default='2-3')

attrs['Secured Loan'] = np.select(condlist=[attrs['Secured High Credit Sum']<=0,
                                             attrs['Secured High Credit Sum']<=200000,
                                             attrs['Secured High Credit Sum']<=750000,
                                             attrs['Secured High Credit Sum']>750000,],choicelist=['No Secured loan','Secured Loan < 2L',
                                                                                        'Secured Loan < 7.5L','Secured Loan 7.5L+'],
                                  default='No Secured loan')

attrs['Income'] = np.select(condlist=[attrs['ctc']<=25000,
                                             attrs['ctc']<=30000,
                                             attrs['ctc']<=35000,
                                            attrs['ctc']<=40000,
                                            attrs['ctc']<=50000,
                                            attrs['ctc']<=75000,
                                             attrs['ctc']>75000],choicelist=['<25000','25000-30000',
                                                                                        '30000-35000','35000-40000','40000-50000',
                                                                                                  '50000-75000','75000+'],\
                            default='25000-30000')

attrs['FOIR_Band'] = np.select(condlist=[attrs['New_FOIR']<=35,
                                             attrs['New_FOIR']<=50,
                                             attrs['New_FOIR']<=60,
                                            attrs['New_FOIR']>60],choicelist=['<=35%','35-50%',
                                                                                        '50-60%','60-70%'],default='35-50%')

#interest_rate
attrs['Time_on_Bureau'] = np.select(condlist=[attrs['AT20S']<=18,
                                             attrs['AT20S']<=24,
                                             attrs['AT20S']<=36,
                                            attrs['AT20S']<=60,
                                            attrs['AT20S']<=84,
                                            attrs['AT20S']>84],
                                    choicelist=['12-18Mths','19-24Mths','2-3 Yrs','3-5 Yrs','5-7 Yrs','7+ Yrs'],
                                    default='3-5 Yrs')

attrs['cust_category'] = np.select(condlist=[((attrs['BC02S']>=1) & (attrs['BC28S']>50000))&((attrs['AU28S']>=200000)|(attrs['MT28S']>=1000000)),\
                                          ((attrs['BC02S']>=1) & (attrs['BC28S']>=50000)),
                                          ((attrs['BC02S']<=0) & (attrs['Unsecured High Credit Sum']>=50000))
                                            ],choicelist=['Carded 50K+ With Secured Loans','Carded 50K+',\
                                                          'Unsecured Loan 50K+'],
                                    default='Consumer Loans Only')

attrs['cust_hirisk'] = np.select(condlist=[((attrs['Unsecured High Credit Sum']<=82000) &
                                            (attrs['CV14_3M']>=2)&
                                           (attrs['AT01S']>=3))
                                            ],choicelist=[1],default=0)

attrs['niro_user_id'] = attrs['user_id'].astype(str).str.strip()

#attrs = attrs[['niro_user_id','income_source','CIBIL','ROI_Band','tenor_band','loan_amt_band','naps_band','supply',\
#               'demand','Income','Imputed_Income','cust_hirisk','Time_on_Bureau','cust_category','riskband','tenure']]

#attrs.to_csv(fileloc+"portcuts.csv")


def preprocessor(df):

    try:
        df['CC_INACTIVE'] = np.select(condlist=[(df['BCPMTSTR']=='INACTIVE')],choicelist=[1],default=0)
        df['CC_NOBC'] = np.select(condlist=[(df['BCPMTSTR']=='NOBC')],choicelist=[1],default=0)
        df['CC_REVOLVER'] = np.select(condlist=[(df['BCPMTSTR']=='REVOLVER')],choicelist=[1],default=0)
        df['CC_RVLRPLUS'] = np.select(condlist=[(df['BCPMTSTR']=='RVLRPLUS')],choicelist=[1],default=0)
        df['CC_TRANSACTOR'] = np.select(condlist=[(df['BCPMTSTR']=='TRANSACTOR')],choicelist=[1],default=0)
        df['CC_TRANPLUS'] = np.select(condlist=[(df['BCPMTSTR']=='TRANPLUS')],choicelist=[1],default=0)
    except:
        print("errors")

    df['credithungry'] = np.select(condlist=[((df['UNSECURED_HIGH_CREDIT_SUM']<=75000)&(df['SECURED_HIGH_CREDIT_SUM']<=0)&\
                                            (df['CV14_6M']>=4))],choicelist=[1],default=0)

    df['bc02s_rnkXbg01s_rnkXcv12_rnkX3091']=np.select(condlist=[((df['BC02S']>=2.0)&(df['BC02S']<=21.0)&(df['BG01S']>=0.0)&(df['BG01S']<=0.0)&(df['CV12']>=1.0)&(df['CV12']<=11.0))],choicelist=[1],default=0)
    df['agg911_rvlr01_6020']=np.select(condlist=[((df['AGG911']>39.22)&(df['AGG911']<=80.38)&(df['RVLR01']>9.21)&(df['RVLR01']<=538.69))],choicelist=[1],default=0)
    df['at01s_g310s_24m__unsecured_high_credit_sum_42']=np.select(condlist=[((df['AT01S']>10)&(df['AT01S']<=284)&(df['G310S_24M']>-0.01)&(df['G310S_24M']<=1)&(df['UNSECURED_HIGH_CREDIT_SUM']>520000)&(df['UNSECURED_HIGH_CREDIT_SUM']<=57875849))],choicelist=[1],default=0)
    df['agg911_rnkXg310s_3m_rnkXat33a_ne_wo_rnkX150']=np.select(condlist=[((df['AGG911']>=0.0)&(df['AGG911']<=34.5)&(df['G310S_3M']>=1.0)&(df['G310S_3M']<=1.0)&(df['AT33A_NE_WO']>=10329.0)&(df['AT33A_NE_WO']<=33080853.0))],choicelist=[1],default=0)
    df['agg911_unsecured_high_credit_sum_3292']=np.select(condlist=[((df['AGG911']>-6.01)&(df['AGG911']<=-1)&(df['UNSECURED_HIGH_CREDIT_SUM']>574040.33)&(df['UNSECURED_HIGH_CREDIT_SUM']<=1418204.33))],choicelist=[1],default=0)
    df['trd_bc28s_9968']=np.select(condlist=[((df['TRD']>-0.01)&(df['TRD']<=4)&(df['BC28S']>-5.01)&(df['BC28S']<=-1))],choicelist=[1],default=0)
    df['g310s_3m_bc106s_60dpd_12m_7369']=np.select(condlist=[((df['G310S_3M']>-0.01)&(df['G310S_3M']<=1)&(df['BC106S_60DPD_12M']>-1.01)&(df['BC106S_60DPD_12M']<=0))],choicelist=[1],default=0)
    df['agg911_rnkXat33a_ne_wo_rnkXsecured_balances_sum_rnkX447']=np.select(condlist=[((df['AGG911']>=38.85)&(df['AGG911']<=1027.37)&(df['AT33A_NE_WO']>=-1.0)&(df['AT33A_NE_WO']<=-1.0)&(df['SECURED_BALANCES_SUM']>=71607.0)&(df['SECURED_BALANCES_SUM']<=495403790.0))],choicelist=[1],default=0)
    df['agg911_rnkXau33s_rnkXcv14_3m_rnkX6945']=np.select(condlist=[((df['AGG911']>=0.0)&(df['AGG911']<=38.84)&(df['AU33S']>=-3.0)&(df['AU33S']<=-1.0)&(df['CV14_3M']>=2.0)&(df['CV14_3M']<=2.0))],choicelist=[1],default=0)
    df['g310s_24m_pl09s_36m_hcsa_le_30_7642']=np.select(condlist=[((df['G310S_24M']>1)&(df['G310S_24M']<=1.5)&(df['PL09S_36M_HCSA_LE_30']>0)&(df['PL09S_36M_HCSA_LE_30']<=1))],choicelist=[1],default=0)
    df['g310s_24m_bc106s_le_30dpd_12m_541']=np.select(condlist=[((df['G310S_24M']>1.5)&(df['G310S_24M']<=9)&(df['BC106S_LE_30DPD_12M']>1)&(df['BC106S_LE_30DPD_12M']<=2))],choicelist=[1],default=0)
    df['cv11_rnkXcv14_12m_rnkXbc107s_12m_rnkX6968']=np.select(condlist=[((df['CV11']>=0.0)&(df['CV11']<=0.0)&(df['CV14_12M']>=8.0)&(df['CV14_12M']<=73.0)&(df['BC107S_12M']>=1.0)&(df['BC107S_12M']<=3.0))],choicelist=[1],default=0)
    df['g310s_3m_cv12_6844']=np.select(condlist=[((df['G310S_3M']>1)&(df['G310S_3M']<=9)&(df['CV12']>0)&(df['CV12']<=16))],choicelist=[1],default=0)
    df['cv11_bc107s_12m_8953']=np.select(condlist=[((df['CV11']>-6.01)&(df['CV11']<=0)&(df['BC107S_12M']>-1)&(df['BC107S_12M']<=0))],choicelist=[1],default=0)
    df['aggs9118'] = np.select(condlist=[((df['AGGS911']>88)&(df['AGGS911']<=600))],choicelist=[1],default=0)
    df['cv14_by_at33a'] = np.select(condlist=[df['AT33A']<=0],choicelist=[df['CV14']],default=df['CV14']/df['AT33A'])
    df['cv14_by_at33a'] = np.select(condlist=[df['cv14_by_at33a']>0.0156,df['cv14_by_at33a']<0.00000001],\
                                                        choicelist=[0.0156,0.00000001],default=df['cv14_by_at33a'])

    df['rvlr01_unsecured_high_credit_sum_6330']=np.select(condlist=[((df['RVLR01']>-6.01)&(df['RVLR01']<=-1)&(df['UNSECURED_HIGH_CREDIT_SUM']>-0.01)&(df['UNSECURED_HIGH_CREDIT_SUM']<=46297.17))],choicelist=[1],default=0)
    df['cv11_rnkXbc02s_rnkXat09s_3m_rnkX1196']=np.select(condlist=[((df['CV11']>=0.0)&(df['CV11']<=0.0)&(df['BC02S']>=0.0)&(df['BC02S']<=1.0)&(df['AT09S_3M']>=2.0)&(df['AT09S_3M']<=22.0))],choicelist=[1],default=0)
    df['at33a_at01s_4682']=np.select(condlist=[((df['AT33A']>-5.01)&(df['AT33A']<=28475.83)&(df['AT01S']>2)&(df['AT01S']<=4))],choicelist=[1],default=0)
    df['rvlr01_rnkXcv14_3m_rnkXunsecured_high_credit_sum_rnkX8903']=np.select(condlist=[((df['RVLR01']>=3.66)&(df['RVLR01']<=148.68)&(df['CV14_3M']>=3.0)&(df['CV14_3M']<=31.0)&(df['UNSECURED_HIGH_CREDIT_SUM']>=123090.0)&(df['UNSECURED_HIGH_CREDIT_SUM']<=565217.0))],choicelist=[1],default=0)
    df['agg911_cv14_www']=np.select(condlist=[((df['AGG911']>-6.01)&(df['AGG911']<=-2)&(df['CV14']>6)&(df['CV14']<=8))],choicelist=[1],default=0)
    df['at20s_cv14_1m']=np.select(condlist=[((df['AT20S']>-5.01)&(df['AT20S']<=24)&(df['CV14_1M']>-0.01)&(df['CV14_1M']<=1))],choicelist=[1],default=0)
    df['cv12_bc107s_24m_3957']=np.select(condlist=[((df['CV12']>0)&(df['CV12']<=16)&(df['BC107S_24M']>0)&(df['BC107S_24M']<=27))],choicelist=[1],default=0)
    df['at33a_g310s']=np.select(condlist=[((df['AT33A']>72909.14)&(df['AT33A']<=145061.71)&(df['G310S']>1)&(df['G310S']<=1.5))],choicelist=[1],default=0)
    df['agg911_rnkXbg01s_rnkXg310s_3m_rnkX571']=np.select(condlist=[((df['AGG911']>=42.59)&(df['AGG911']<=113.37)&(df['BG01S']>=1.0)&(df['BG01S']<=9.0)&(df['G310S_3M']>=1.5)&(df['G310S_3M']<=9.0))],choicelist=[1],default=0)
    df['trd_at01s_8980']=np.select(condlist=[((df['TRD']>24)&(df['TRD']<=380)&(df['AT01S']>2)&(df['AT01S']<=4))],choicelist=[1],default=0)
    df['agg911_rnkXpl28s_rnkXg310s_3m_rnkX2111']=np.select(condlist=[((df['AGG911']>=39.1)&(df['AGG911']<=178.39)&(df['PL28S']>=3600.0)&(df['PL28S']<=200000.0)&(df['G310S_3M']>=1.5)&(df['G310S_3M']<=9.0))],choicelist=[1],default=0)
    df['cv11_cv14_12m_4']=np.select(condlist=[((df['CV11']>-6.01)&(df['CV11']<=0)&(df['CV14_12M']>5)&(df['CV14_12M']<=7))],choicelist=[1],default=0)
    df['g310s_24m_rvlr01_1387']=np.select(condlist=[((df['G310S_24M']>1.5)&(df['G310S_24M']<=9)&(df['RVLR01']>-6.01)&(df['RVLR01']<=-1))],choicelist=[1],default=0)
    df['trd_pl33s_6726']=np.select(condlist=[((df['TRD']>15)&(df['TRD']<=24)&(df['PL33S']>247.67)&(df['PL33S']<=51449))],choicelist=[1],default=0)
    df['bc02s_at33a_5298']=np.select(condlist=[((df['BC02S']>-1)&(df['BC02S']<=1)&(df['AT33A']>-5.01)&(df['AT33A']<=28475.83))],choicelist=[1],default=0)
    return df

def getNapsScore(row):
    
    zz = (-1.7249)+(row['cv11_bc107s_12m_8953']*-0.6572)+(row['G310S_6M']*-0.3441)+(row['g310s_24m_rvlr01_1387']*0.8992)+\
         (row['bc02s_rnkXbg01s_rnkXcv12_rnkX3091']*1.0623)+(row['aggs9118']*0.6604)+(row['CV14_6M']*0.0361)+\
         (row['agg911_rvlr01_6020']*-0.8515)+(row['cv14_by_at33a']*31.4963)+(row['cv11_cv14_12m_4']*-0.452)+\
         (row['at01s_g310s_24m__unsecured_high_credit_sum_42']*-0.7234)+(row['rvlr01_unsecured_high_credit_sum_6330']*0.3369)+\
         (row['agg911_rnkXg310s_3m_rnkXat33a_ne_wo_rnkX150']*2.9448)+(row['cv11_rnkXbc02s_rnkXat09s_3m_rnkX1196']*0.6413)+\
         (row['agg911_unsecured_high_credit_sum_3292']*-0.7401)+(row['at33a_at01s_4682']*-0.5306)+(row['trd_pl33s_6726']*-1.0659)+\
         (row['trd_bc28s_9968']*0.3131)+(row['rvlr01_rnkXcv14_3m_rnkXunsecured_high_credit_sum_rnkX8903']*1.3062)+\
         (row['g310s_3m_bc106s_60dpd_12m_7369']*-0.6157)+(row['agg911_cv14_www']*0.8942)+(row['bc02s_at33a_5298']*0.5325)+\
         (row['agg911_rnkXat33a_ne_wo_rnkXsecured_balances_sum_rnkX447']*-0.4146)+(row['at20s_cv14_1m']*0.2631)+\
         (row['agg911_rnkXau33s_rnkXcv14_3m_rnkX6945']*-1.5711)+(row['cv12_bc107s_24m_3957']*-1.1005)+\
         (row['g310s_24m_pl09s_36m_hcsa_le_30_7642']*0.472)+(row['at33a_g310s']*0.6542)+(\
             row['g310s_24m_bc106s_le_30dpd_12m_541']*0.8891)+(row['agg911_rnkXbg01s_rnkXg310s_3m_rnkX571']*1.6077)+\
             (row['cv11_rnkXcv14_12m_rnkXbc107s_12m_rnkX6968']*1.6329)+(row['trd_at01s_8980']*1.9295)+\
             (row['g310s_3m_cv12_6844']*0.9481)+(row['agg911_rnkXpl28s_rnkXg310s_3m_rnkX2111']*1.0015)

    r = np.exp(zz)/(1+np.exp(zz))
    scr = int(max(350,min(575*(r**(-0.08)),900)))

    return scr

attrs['SECURED_ACCOUNTS_COUNT']=attrs['Secured Accounts Count']
attrs['UNSECURED_ACCOUNTS_COUNT']=attrs['Unsecured Accounts Count']
attrs['SECURED_HIGH_CREDIT_SUM']=attrs['Secured High Credit Sum']
attrs['UNSECURED_HIGH_CREDIT_SUM']=attrs['Unsecured High Credit Sum']
attrs['SECURED_AMOUNT_OVERDUE_SUM']=attrs['Secured Amount Overdue Sum']
attrs['UNSECURED_AMOUNT_OVERDUE_SUM']=attrs['Unsecured Amount Overdue Sum']
attrs['SECURED_BALANCES_SUM']=attrs['Secured Balances Sum']
attrs['UNSECURED_BALANCES_SUM']=attrs['Unsecured Balances Sum']
              
attrs = preprocessor(attrs)
attrs['naps_new']= attrs.apply(getNapsScore,axis=1)
attrs['naps_new_bkt'] = attrs.apply(naps_score_bkt,var='naps_new',axis=1)

perf_full = pd.DataFrame()
for i in range(len(perf)):
    a = pd.DataFrame()
    #print("start:",perf.loc[i,'disb_yymm'])
    disb = str(perf.loc[i,'disb_yymm'].year)+("0"+str(perf.loc[i,'disb_yymm'].month))[-2:]
    for mob in range(perf.loc[i,'months_since_disb']):
        thisdt = perf.loc[i,'disb_yymm']+relativedelta(months=mob)
        #print("        this:",thisdt)
        month = str(thisdt.year)+("0"+str(thisdt.month))[-2:]
        #pos = 0

        ptr = 12*(thisdt.year-perf_startdate.year)+(thisdt.month-perf_startdate.month)
        xPlusDPD = 0
        thirtyPlusDPD = 0
        sixtyPlusDPD = 0
        ninetyPlusDPD = 0
        bounced = 0
        POSactive = 1

        if ptr>=0:
            dpd = perf.loc[i,'DPD'+str(ptr)]
            POS = perf.loc[i,'Prin'+str(ptr)]
            try:
                recovery = perf.loc[i,'recovery']
            except:
                recovery = 0
                
            if POS>0:
                POSactive = 1
            else:
                POSactive = 0
            presStatus = perf.loc[i,'Presentation'+str(ptr)]

            if 'BOUNCE' in str(presStatus).upper():
                bounced=1
            
            if dpd=='90+':
                xPlusDPD = 1
                thirtyPlusDPD = 1
                sixtyPlusDPD = 1
                ninetyPlusDPD = 1
            elif dpd=='Apr90+':
                xPlusDPD = 1
                thirtyPlusDPD = 1
                sixtyPlusDPD = 1
                ninetyPlusDPD = 1
            elif (('NPA' in str(dpd)) or (dpd=='NPA (Unsettled)') or (dpd=='NPA (unsettled)') or (dpd=='Settled')):
                xPlusDPD = 1
                thirtyPlusDPD = 1
                sixtyPlusDPD = 1
                ninetyPlusDPD = 1
            elif dpd=='60+':
                xPlusDPD = 1
                thirtyPlusDPD = 1
                sixtyPlusDPD = 1
                ninetyPlusDPD = 0
            elif dpd=='30+':
                xPlusDPD = 1
                thirtyPlusDPD = 1
                sixtyPlusDPD = 0
                ninetyPlusDPD = 0
            elif dpd=='1-29+':
                xPlusDPD = 1
                thirtyPlusDPD = 0
                sixtyPlusDPD = 0
                ninetyPlusDPD = 0
                
        else:
            cntr = 0
            dpd = '0'
            POS = perf.loc[i,'Principal']
            recovery = 0

        if (ninetyPlusDPD>0):
            POS = POS - recovery
            
        xPlusPOS = xPlusDPD*POS
        thirtyPlusPOS = thirtyPlusDPD*POS
        sixtyPlusPOS = sixtyPlusDPD*POS
        ninetyPlusPOS = ninetyPlusDPD*POS
        bouncedPOS = bounced*POS
        
        tmp = pd.DataFrame({'niro_user_id':[perf.loc[i,'niro_user_id']],'mob':[mob],'disb_dt':[disb],'month':[month],\
                          'POS':[POS],'xPlusDPD':[xPlusDPD],'thirtyPlusDPD':[thirtyPlusDPD],\
                            'sixtyPlusDPD':[sixtyPlusDPD],'ninetyPlusDPD':[ninetyPlusDPD],'xPlusPOS':[xPlusPOS],\
                            'thirtyPlusPOS':[thirtyPlusPOS],'sixtyPlusPOS':[sixtyPlusPOS],\
                            'ninetyPlusPOS':[ninetyPlusPOS],'bounced':[bounced],'bouncedPOS':[bouncedPOS],'POSactive':[POSactive],\
                            'Loan_Amount':[perf.loc[i,'Principal']],'EMIt':[perf.loc[i,'Monthly Emi']],'Loan_Tenor':[perf.loc[i,'Tenor In Months']],\
                            'Loan_ROI':[perf.loc[i,'Interest Rate']],'Loan_PF':[perf.loc[i,'PF']],\
                            'City':[perf.loc[i,'City']],'State':[perf.loc[i,'State']],'Aget':[perf.loc[i,'Age']],\
                            'Gender':[perf.loc[i,'Gender']],'pincode':[perf.loc[i,'Pin Code']]})
        a = pd.concat([a,tmp])
    perf_full  = pd.concat([perf_full,a])
perf_full['POS'].fillna(0,inplace=True)
perf_full['xPlusPOS'].fillna(0,inplace=True)
perf_full['thirtyPlusPOS'].fillna(0,inplace=True)
perf_full['sixtyPlusPOS'].fillna(0,inplace=True)
perf_full['ninetyPlusPOS'].fillna(0,inplace=True)
attrs.drop_duplicates(subset=['niro_user_id'], keep='first',inplace=True)
perf_full = perf_full.merge(attrs,how='inner',on='niro_user_id')

pinmap = pd.read_csv(commloc+"pincodemap2.csv",low_memory=False)
pinmap.drop_duplicates(subset=['pincode'], keep='first',inplace=True)
perf_full = perf_full.merge(pinmap,how='left',on='pincode')
perf_full.fillna(method='ffill',inplace=True)

perf_full['EMI'] = pd.to_numeric(perf_full["EMIt"].str.replace(",","").str.replace("-","").str.strip())
perf_full['EMI_Band'] = np.select(condlist=[perf_full['EMI']<=6500,
                                             perf_full['EMI']<=8500,
                                             perf_full['EMI']<=13000,
                                            perf_full['EMI']<=18000,
                                            perf_full['EMI']>18000
                                            ],choicelist=['<=6500','6500-8500','8500-13000',\
                                                          '13000-18000','18000+'],default='8500-13000')
#interest_rate
perf_full['ROI_Band'] = np.select(condlist=[perf_full['Loan_ROI']<=19,
                                             perf_full['Loan_ROI']<=22,
                                             perf_full['Loan_ROI']<=24,
                                            perf_full['Loan_ROI']<=26
                                            ],choicelist=['<=19','19-22','22-24','24-26'],default='26-29')

perf_full['tenor_band'] = np.select(condlist=[perf_full['Loan_Tenor']<=12,
                                             perf_full['Loan_Tenor']<=18,
                                             perf_full['Loan_Tenor']<=24,
                                             perf_full['Loan_Tenor']<=36,
                                             perf_full['Loan_Tenor']>36,],choicelist=['<=12','13-18',
                                                                                        '19-24','25-36',
                                                                                        '37+'],default='13-18')

perf_full['mobxloan'] = perf_full['mob']*perf_full['Loan_Amount']
perf_full['tenorxloan'] = perf_full['Loan_Tenor']*perf_full['Loan_Amount']
perf_full['roixloan'] = perf_full['Loan_ROI']*perf_full['Loan_Amount']
perf_full['pfxloan'] = perf_full['Loan_PF']*perf_full['Loan_Amount']
#perf_full['Age'] = pd.to_numeric(perf_full["Aget"].str.replace(",","").str.replace("-","").str.strip())
perf_full['Age_Band'] = np.select(condlist=[perf_full['Aget']<=25,
                                             perf_full['Aget']<=30,
                                             perf_full['Aget']<=35,
                                            perf_full['Aget']<=40,
                                            perf_full['Aget']<=45,
                                            perf_full['Aget']>45
                                            ],choicelist=['<=25','26-30','31-35',\
                                                          '36-40','41-45','46+'],default='36-40')

perf_full['loan_amt_band'] = np.select(condlist=[perf_full['Loan_Amount']<=75000,
                                             perf_full['Loan_Amount']<=100000,
                                             perf_full['Loan_Amount']<=200000,
                                             perf_full['Loan_Amount']<=300000,
                                             perf_full['Loan_Amount']<=400000,
                                             perf_full['Loan_Amount']<=500000,
                                             perf_full['Loan_Amount']>500000,],choicelist=['<=75K','75K-1L','1L-2L','2L-3L','3L-4L',
                                                                                        '4L-5L','5L+'],default='1L-2L')



perf_6mob = perf_full[perf_full['mob']==4]
perf_6mob.to_csv(fileloc+'tree4.csv')

perf_full.to_csv(fileloc+"perf_full_rac.csv")
q = perf_full.groupby(['disb_dt','month','mob','income_source','CIBIL','supply','demand','loan_amt_band',\
                       'Income','cust_category','riskband','tenor_band','naps_band','naps_new_bkt','Credit cards','Time_on_Bureau',\
                       'City','Age_Band','Inq_12m','cust_hirisk']).agg({'niro_user_id':'count','Loan_Amount':'sum','POS':'sum','mobxloan':'sum',\
                                             'bounced':'sum','xPlusDPD':'sum','thirtyPlusDPD':'sum','sixtyPlusDPD':'sum',\
                                                            'ninetyPlusDPD':'sum','bouncedPOS':'sum',\
                                                            'xPlusPOS':'sum','thirtyPlusPOS':'sum','sixtyPlusPOS':'sum',\
                                                            'ninetyPlusPOS':'sum','POSactive':'sum','tenorxloan':'sum',\
                                                                                  'roixloan':'sum','pfxloan':'sum'})

q.to_csv(fileloc+"perfcheck_rac.csv")
