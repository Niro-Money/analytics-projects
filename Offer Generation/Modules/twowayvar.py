import random
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
from __init__ import *
# df = pd.read_csv("F:/Niro/NAPS v3/Calibrations/Housing CL DS6 TLI.csv",low_memory=False)
#df = pd.read_csv("F:/Niro/NAPS v3/Calibrations/FINBUD DS2 TLI.csv",low_memory=False)

####### Define odd even based on Phone Number please


def score_bkt(row, var):
    if row[var] <= 600:
        return '<=600'
    elif row[var] <= 620:
        return '601-620'
    elif row[var] <= 640:
        return '621-640'
    elif row[var] <= 660:
        return '641-660'
    elif row[var] <= 700:
        return '661-700'
    elif row[var] <= 710:
        return '701-710'
    elif row[var] <= 720:
        return '711-720'
    elif row[var] <= 730:
        return '721-730'
    elif row[var] <= 740:
        return '731-740'
    elif row[var] <= 750:
        return '741-750'
    elif row[var] <= 760:
        return '751-760'
    elif row[var] <= 770:
        return '761-770'
    elif row[var] <= 780:
        return '771-780'
    elif row[var] <= 790:
        return '781-790'
    elif row[var] <= 800:
        return '791-800'
    else:
        return '800+'

def lenderAssignment(data: pd.DataFrame) -> pd.DataFrame:
    data["lending partner"] = np.select(
        condlist=[
            (data["riskband"] == "CAT-D")|(data['salary']<30000),
            ((data["riskband"] == "CAT-C")&(data['subrisk']=='Hig')),
            (data["payu_serviceable"] == "yes"),
            ],
        choicelist=["Liquiloans","Liquiloans", "PayU"],
        default="Liquiloans",
    )
  
    return data


def apply_bureau(row):
    # if row['pincode_Approved']==0:
    #    return 'pin do not match to niro-pin'
    if row['pan_no_checked'] == 'wrong pan':
        return 'PAN Error'
    elif row['calculated_age'] < 21:
        return 'age<21'
    # elif row['Calculated_Age'] > 57:
    #     return 'age>57'
    elif row['gender'] == 'Transgender':
        return 'Transgender'
    elif row['cibiltusc3_score_value'] < 700:
        return 'CIBIL_LT_700'
    elif row['at20s'] < 12:
        return 'Months on Bureau<12'
    elif row['cv11_24m']+row['cv12'] > 0:
        return '1+ Trade60+ in 2Y'
    elif row['g310s']>=2:
        return '1+ Trade 60+ ever'
    elif row['g310s_6m'] > 1.5:
        return '1+ Trade30+ in 6M'
    elif row['g310s_3m'] > 1:
        return '1+ Trade 1DPD+ in 3M'
    elif row['co01s180'] > 0:
        return 'Past Write-off'
    elif row['cv14_6m'] >= 8:
        return '10+ Inq in lst 6Mth'
    elif row['at09s_3m'] >= 2:
        return '2+ Trades opened in last 3Month'
    elif (row['salary'] <= 20000) :
        return 'salary<20K'
    elif (row['freeIncome'] <= 2000) :
        return 'free Income<2K'
    elif (row['foir'] >= 0.65) :
        return 'foir>65%'
    elif (row['naps'] < 700) :
        return 'NAPS<700'
    else:
        return 'NOT DECLINED'

def apply_bureau_post(row):
    # if row['pincode_Approved']==0:
    #    return 'pin do not match to niro-pin'
    if row['decline'] == 'NOT DECLINED':
        if row['finalEMI']<=2500:
            return 'Low Amount Borderline'
        elif ((row['finalEMI']+row['emi'])/(1+row['salary']))>0.7:
            return 'foir>70%'
        else:
            return 'NOT DECLINED'
    else:
        return row['decline']

def borderCases(row):
    if (row['agg911'] > 95):
        return '90+% Bank Card Utilization in last 12 month'
    elif (row['stated_income'] <= 35000) :
        return 'salary<35K'
    elif (row['freeIncome'] <= 4000) :
        return 'free Income<4K'
    elif (row['foir'] >= 0.7) :
        return 'foir>70%'
    elif (row['bc28s'] < 25000) and (row['unsecured_high_credit_sum']<=50000) and (row['secured_high_credit_sum']<=100000) :
        return 'No Hi-Sanction Made'
    else:
        return 'clean'
                                         


def card_bkt(row):
    if row['bc02s'] <= 0:
        return 'Not Carded'
    elif row['bc02s'] <= 1:
        return 'One Card'
    else:
        return 'Mutiple Cards'

def preprocessor(df):
    df['cibilv3_bkt']= df.apply(score_bkt,var='cibiltusc3_score_value',axis=1)

    try:
        df['CC_INACTIVE'] = np.select(condlist=[(df['bcpmtstr']=='INACTIVE')],choicelist=[1],default=0)
        df['CC_NOBC'] = np.select(condlist=[(df['bcpmtstr']=='NOBC')],choicelist=[1],default=0)
        df['CC_REVOLVER'] = np.select(condlist=[(df['bcpmtstr']=='REVOLVER')],choicelist=[1],default=0)
        df['CC_RVLRPLUS'] = np.select(condlist=[(df['bcpmtstr']=='RVLRPLUS')],choicelist=[1],default=0)
        df['CC_TRANSACTOR'] = np.select(condlist=[(df['bcpmtstr']=='TRANSACTOR')],choicelist=[1],default=0)
        df['CC_TRANPLUS'] = np.select(condlist=[(df['bcpmtstr']=='TRANPLUS')],choicelist=[1],default=0)
    except:
        print("errors")

    df['credithungry'] = np.select(condlist=[((df['unsecured_high_credit_sum']<=75000)&(df['secured_high_credit_sum']<=0)&\
                                            (df['cv14_6m']>=4))],choicelist=[1],default=0)

    df['bc02s_rnkXbg01s_rnkXcv12_rnkX3091']=np.select(condlist=[((df['bc02s']>=2.0)&(df['bc02s']<=21.0)&(df['bg01s']>=0.0)&(df['bg01s']<=0.0)&(df['cv12']>=1.0)&(df['cv12']<=11.0))],choicelist=[1],default=0)
    df['agg911_rvlr01_6020']=np.select(condlist=[((df['agg911']>39.22)&(df['agg911']<=80.38)&(df['rvlr01']>9.21)&(df['rvlr01']<=538.69))],choicelist=[1],default=0)
    df['at01s_g310s_24m__unsecured_high_credit_sum_42']=np.select(condlist=[((df['at01s']>10)&(df['at01s']<=284)&(df['g310s_24m']>-0.01)&(df['g310s_24m']<=1)&(df['unsecured_high_credit_sum']>520000)&(df['unsecured_high_credit_sum']<=57875849))],choicelist=[1],default=0)
    df['agg911_rnkXg310s_3m_rnkXat33a_ne_wo_rnkX150']=np.select(condlist=[((df['agg911']>=0.0)&(df['agg911']<=34.5)&(df['g310s_3m']>=1.0)&(df['g310s_3m']<=1.0)&(df['at33a_ne_wo']>=10329.0)&(df['at33a_ne_wo']<=33080853.0))],choicelist=[1],default=0)
    df['agg911_unsecured_high_credit_sum_3292']=np.select(condlist=[((df['agg911']>-6.01)&(df['agg911']<=-1)&(df['unsecured_high_credit_sum']>574040.33)&(df['unsecured_high_credit_sum']<=1418204.33))],choicelist=[1],default=0)
    df['trd_bc28s_9968']=np.select(condlist=[((df['trd']>-0.01)&(df['trd']<=4)&(df['bc28s']>-5.01)&(df['bc28s']<=-1))],choicelist=[1],default=0)
    df['g310s_3m_bc106s_60dpd_12m_7369']=np.select(condlist=[((df['g310s_3m']>-0.01)&(df['g310s_3m']<=1)&(df['bc106s_60dpd_12m']>-1.01)&(df['bc106s_60dpd_12m']<=0))],choicelist=[1],default=0)
    df['agg911_rnkXat33a_ne_wo_rnkXsecured_balances_sum_rnkX447']=np.select(condlist=[((df['agg911']>=38.85)&(df['agg911']<=1027.37)&(df['at33a_ne_wo']>=-1.0)&(df['at33a_ne_wo']<=-1.0)&(df['secured_balances_sum']>=71607.0)&(df['secured_balances_sum']<=495403790.0))],choicelist=[1],default=0)
    df['agg911_rnkXau33s_rnkXcv14_3m_rnkX6945']=np.select(condlist=[((df['agg911']>=0.0)&(df['agg911']<=38.84)&(df['au33s']>=-3.0)&(df['au33s']<=-1.0)&(df['cv14_3m']>=2.0)&(df['cv14_3m']<=2.0))],choicelist=[1],default=0)
    df['g310s_24m_pl09s_36m_hcsa_le_30_7642']=np.select(condlist=[((df['g310s_24m']>1)&(df['g310s_24m']<=1.5)&(df['pl09s_36m_hcsa_le_30']>0)&(df['pl09s_36m_hcsa_le_30']<=1))],choicelist=[1],default=0)
    df['g310s_24m_bc106s_le_30dpd_12m_541']=np.select(condlist=[((df['g310s_24m']>1.5)&(df['g310s_24m']<=9)&(df['bc106s_le_30dpd_12m']>1)&(df['bc106s_le_30dpd_12m']<=2))],choicelist=[1],default=0)
    df['cv11_rnkXcv14_12m_rnkXbc107s_12m_rnkX6968']=np.select(condlist=[((df['cv11']>=0.0)&(df['cv11']<=0.0)&(df['cv14_12m']>=8.0)&(df['cv14_12m']<=73.0)&(df['bc107s_12m']>=1.0)&(df['bc107s_12m']<=3.0))],choicelist=[1],default=0)
    df['g310s_3m_cv12_6844']=np.select(condlist=[((df['g310s_3m']>1)&(df['g310s_3m']<=9)&(df['cv12']>0)&(df['cv12']<=16))],choicelist=[1],default=0)
    df['cv11_bc107s_12m_8953']=np.select(condlist=[((df['cv11']>-6.01)&(df['cv11']<=0)&(df['bc107s_12m']>-1)&(df['bc107s_12m']<=0))],choicelist=[1],default=0)
    df['aggs9118'] = np.select(condlist=[((df['aggs911']>88)&(df['aggs911']<=600))],choicelist=[1],default=0)
    df['cv14_by_at33a'] = np.select(condlist=[df['at33a']<=0],choicelist=[df['cv14']],default=df['cv14']/df['at33a'])
    df['cv14_by_at33a'] = np.select(condlist=[df['cv14_by_at33a']>0.0156,df['cv14_by_at33a']<0.00000001],\
                                                        choicelist=[0.0156,0.00000001],default=df['cv14_by_at33a'])

    df['rvlr01_unsecured_high_credit_sum_6330']=np.select(condlist=[((df['rvlr01']>-6.01)&(df['rvlr01']<=-1)&(df['unsecured_high_credit_sum']>-0.01)&(df['unsecured_high_credit_sum']<=46297.17))],choicelist=[1],default=0)
    df['cv11_rnkXbc02s_rnkXat09s_3m_rnkX1196']=np.select(condlist=[((df['cv11']>=0.0)&(df['cv11']<=0.0)&(df['bc02s']>=0.0)&(df['bc02s']<=1.0)&(df['at09s_3m']>=2.0)&(df['at09s_3m']<=22.0))],choicelist=[1],default=0)
    df['at33a_at01s_4682']=np.select(condlist=[((df['at33a']>-5.01)&(df['at33a']<=28475.83)&(df['at01s']>2)&(df['at01s']<=4))],choicelist=[1],default=0)
    df['rvlr01_rnkXcv14_3m_rnkXunsecured_high_credit_sum_rnkX8903']=np.select(condlist=[((df['rvlr01']>=3.66)&(df['rvlr01']<=148.68)&(df['cv14_3m']>=3.0)&(df['cv14_3m']<=31.0)&(df['unsecured_high_credit_sum']>=123090.0)&(df['unsecured_high_credit_sum']<=565217.0))],choicelist=[1],default=0)
    df['agg911_cv14_www']=np.select(condlist=[((df['agg911']>-6.01)&(df['agg911']<=-2)&(df['cv14']>6)&(df['cv14']<=8))],choicelist=[1],default=0)
    df['at20s_cv14_1m']=np.select(condlist=[((df['at20s']>-5.01)&(df['at20s']<=24)&(df['cv14_1m']>-0.01)&(df['cv14_1m']<=1))],choicelist=[1],default=0)
    df['cv12_bc107s_24m_3957']=np.select(condlist=[((df['cv12']>0)&(df['cv12']<=16)&(df['bc107s_24m']>0)&(df['bc107s_24m']<=27))],choicelist=[1],default=0)
    df['at33a_g310s']=np.select(condlist=[((df['at33a']>72909.14)&(df['at33a']<=145061.71)&(df['g310s']>1)&(df['g310s']<=1.5))],choicelist=[1],default=0)
    df['agg911_rnkXbg01s_rnkXg310s_3m_rnkX571']=np.select(condlist=[((df['agg911']>=42.59)&(df['agg911']<=113.37)&(df['bg01s']>=1.0)&(df['bg01s']<=9.0)&(df['g310s_3m']>=1.5)&(df['g310s_3m']<=9.0))],choicelist=[1],default=0)
    df['trd_at01s_8980']=np.select(condlist=[((df['trd']>24)&(df['trd']<=380)&(df['at01s']>2)&(df['at01s']<=4))],choicelist=[1],default=0)
    df['agg911_rnkXpl28s_rnkXg310s_3m_rnkX2111']=np.select(condlist=[((df['agg911']>=39.1)&(df['agg911']<=178.39)&(df['pl28s']>=3600.0)&(df['pl28s']<=200000.0)&(df['g310s_3m']>=1.5)&(df['g310s_3m']<=9.0))],choicelist=[1],default=0)
    df['cv11_cv14_12m_4']=np.select(condlist=[((df['cv11']>-6.01)&(df['cv11']<=0)&(df['cv14_12m']>5)&(df['cv14_12m']<=7))],choicelist=[1],default=0)
    df['g310s_24m_rvlr01_1387']=np.select(condlist=[((df['g310s_24m']>1.5)&(df['g310s_24m']<=9)&(df['rvlr01']>-6.01)&(df['rvlr01']<=-1))],choicelist=[1],default=0)
    df['trd_pl33s_6726']=np.select(condlist=[((df['trd']>15)&(df['trd']<=24)&(df['pl33s']>247.67)&(df['pl33s']<=51449))],choicelist=[1],default=0)
    df['bc02s_at33a_5298']=np.select(condlist=[((df['bc02s']>-1)&(df['bc02s']<=1)&(df['at33a']>-5.01)&(df['at33a']<=28475.83))],choicelist=[1],default=0)
    return df
def getNapsScore(row):
    
    zz = (-1.7249)+(row['cv11_bc107s_12m_8953']*-0.6572)+(row['g310s_6m']*-0.3441)+(row['g310s_24m_rvlr01_1387']*0.8992)+\
         (row['bc02s_rnkXbg01s_rnkXcv12_rnkX3091']*1.0623)+(row['aggs9118']*0.6604)+(row['cv14_6m']*0.0361)+\
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

def getIncome(row):

    '''
    inc = (79365.988224)+(row['bc02s']*8350.837584)+(row['bc107s_24m']*8616.396829)+(row['bc107s_12m']*9385.960832)+\
          (row['mt01s']*13847.476953)+(row['CC_RVLRPLUS']*18784.403002)+(row['bg01s']*2677.624573)+\
          (row['secured_accounts_count']*547.318198)+(row['CC_REVOLVER']*15268.666404)+(row['g310s_24m']*-13177.228397)+\
          (row['CC_TRANSACTOR']*4751.46118)+(row['CC_INACTIVE']*3116.883405)+(row['cv11']*-10168.999152)+(row['cv13']*-626.581373)+\
          (row['credithungry']*-10273.372748)
    '''
    
    inc = (39375.611014)+(row['bc02s']*7693.164038)+(row['bc107s_12m']*-6202.735389)+(row['mt01s']*12829.755379)+\
          (row['CC_RVLRPLUS']*5283.620429)+(row['CC_NOBC']*2641.422428)+(row['bg01s']*2470.48576)+\
          (row['secured_accounts_count']*876.713522)+(row['CC_REVOLVER']*7000.266735)+(row['g310s_24m']*-13853.134557)+\
          (row['cv10']*9487.837402)+(row['cv13']*-629.31684)+(row['credithungry']*-14694.253505)+\
          (row['agg911_rvlr01_6020']*40266.608384)+(row['cv11_bc107s_12m_8953']*48715.297269)+\
          (row['g310s_24m_rvlr01_1387']*-36779.102132)+(row['aggs9118']*-37937.273498)

    return int(max(5000,min(inc,200000))/100)*100

def getCxStatedIncome(row):
    
    inc = (36290)+(row['unsecured_high_credit_sum']*0.0069)+(row['unsecured_balances_sum']*0.009)+(row['at33a']*-0.0008)+\
          (row['at20s']*49.4329)+(row['cv14']*-80.2239)+(row['au28s']*0.014)+(row['secured_high_credit_sum']*0.0021)+\
          (row['mt01s']*1166.707)+(row['CC_NOBC']*-4504.749)+(row['cv14_12m']*-293.7137)+(row['bg01s']*4295.0622)+\
          (row['CC_TRANPLUS']*2522.5409)+(row['CC_TRANSACTOR']*2064.8728)+(row['at09s_12m']*-187.9049)+\
          (row['credithungry']*-3840.1473)+(row['agg911_rvlr01_6020']*-3149.3997)

    return int(max(5000,min(inc,200000))/100)*100

def getEMI(row):
    emi = 0
    if row['pl33s']>0:
        emi = emi + max(0,row['pl28s'])*0.035
    if row['au33s']>0:
        emi = emi + max(0,row['au28s'])*0.02
    if row['mt33s']>0:
        emi = emi + max(0,row['mt28s'])*0.012
    if row['bc02s']>0:
        emi = emi + max(0,row['bc28s']*row['agg911']*0.01*0.05)

    return int(emi)

def getRiskBand(row):
    if row['naps']>=775 and row['cibiltusc3_score_value']>=770 and row['incomeRules']=='clean':
        return 'CAT-A'
    elif row['naps']>=745 and row['cibiltusc3_score_value']>=740 and row['incomeRules']=='clean':
        return 'CAT-B'
    elif row['naps']>=730 and row['cibiltusc3_score_value']>=720:
        return 'CAT-C'
    elif row['naps']>=700 and row['cibiltusc3_score_value']>=700:
        return 'CAT-D'
    else:
        return 'JUNK'

def getSubRiskBand(row):
    if row['riskband']=='CAT-A':
        if row['cibiltusc3_score_value']>=785 and row['bc28s']>=100000:
            return "Low"
        elif row['cibiltusc3_score_value']>=775 and row['bc28s']>=25000:
            return "Med"
        else:
            return "Hig"
    elif row['riskband']=='CAT-B':
        if row['cibiltusc3_score_value']>=780 and row['bc28s']>=100000:
            return "Low"
        elif row['cibiltusc3_score_value']>=730 and row['bc28s']>=25000:
            return "Med"
        else:
            return "Hig"
    elif row['riskband']=='CAT-C':
        if row['cibiltusc3_score_value']>=730 and row['bc28s']>=50000 and row['incomeRules']=='clean':
            return "Low"
        elif row['cibiltusc3_score_value']>=710 and row['bc28s']>=10000 and row['incomeRules']=='clean':
            return "Med"
        else:
            return "Hig"
    else:
        if row['cibiltusc3_score_value']>=720 and row['bc28s']>=10000 and row['incomeRules']=='clean':
            return "Low"
        elif row['cibiltusc3_score_value']>=740 and row['unsecured_high_credit_sum']>=50000 and row['incomeRules']=='clean':
            return "Med"
        else:
            return "Hig"

def getMaxTenor(row):

    if row['riskband']=='CAT-A':
        if row['lending partner'] == 'PayU':
            if row['subrisk']=="Low":
                return 36
            elif row['subrisk']=="Med":
                return 36
            else:
                return 30
        else:
            return 24
    elif row['riskband']=='CAT-B':
        if row['lending partner'] == 'PayU':
            if row['subrisk']=="Low":
                return 36
            elif row['subrisk']=="Med":
                return 30
            else:
                return 24
        else:
            return 24
    elif row['riskband']=='CAT-C':
        if row['subrisk']=="Low":
            return 24
        elif row['subrisk']=="Med" and row['odd_test']==0:
            return 24
        elif row['subrisk']=="Med" and row['odd_test']==1:
            return 18
        elif row['odd_test']==0:
            return 18
        else:
            return 12
    else:
        if row['subrisk']=="Low":
            return 12
        elif row['subrisk']=="Med":
            return 6
        else:
            return 4

def getMaxEMI(row):
    if row['riskband']=='CAT-A':
        if row['subrisk']=="Low":
            return 15000
        elif row['subrisk']=="Med":
            return 13000
        else:
            return 11000
    elif row['riskband']=='CAT-B':
        if row['subrisk']=="Low":
            return 10000
        elif row['subrisk']=="Med":
            return 10000
        else:
            return 8500
    elif row['riskband']=='CAT-C':
        if row['subrisk']=="Low":
            return 8500
        elif row['subrisk']=="Med" and row['odd_test']==0:
            return 5000
        elif row['subrisk']=="Med" and row['odd_test']==1:
            return 7000
        elif row['odd_test']==0:
            return 3500
        else:
            return 6500
    elif row['riskband']=='CAT-D':
        if row['subrisk']=="Low":
            return 4000
        elif row['subrisk']=="Med":
            return 4000
        else:
            return 3500
    else:
        return 0

def getMaxAmount(row):
    if row['riskband']=='CAT-A':
        if row['lending partner'] == 'PayU':
    
            if row['subrisk']=="Low":
                return 500000
            elif row['subrisk']=="Med":
                return 400000
            else:
                return 300000
        else: 
            return 240000
    elif row['riskband']=='CAT-B':
        if row['lending partner'] == 'PayU':
            if row['subrisk']=="Low":
                return 400000
            elif row['subrisk']=="Med":
                return 300000
            else:
                return 200000
        else:
            if row['subrisk']=="Low":
                return 240000
            elif row['subrisk']=="Med":
                return 240000
            else:
                return 200000
    elif row['riskband']=='CAT-C':
        if row['subrisk']=="Low":
            return 200000
        elif row['subrisk']=="Med":
            return 120000
        else:
            return 75000
    elif row['riskband']=='CAT-D':
        if row['subrisk']=="Low":
            return 50000
        elif row['subrisk']=="Med":
            return 25000
        else:
            return 20000
    else:
        return 0

def getROI(row):
    if row['riskband']=='CAT-A':
        if row['subrisk']=="Low":
            return 17.9
        elif row['subrisk']=="Med":
            return 18.9
        else:
            return 19.9
    elif row['riskband']=='CAT-B':
        if row['subrisk']=="Low":
            return 19.9
        elif row['subrisk']=="Med":
            return 21.9
        else:
            return 22.9
    elif row['riskband']=='CAT-C':
        if row['subrisk']=="Low":
            return 23.9
        elif row['subrisk']=="Med":
            return 24.9
        else:
            return 25.9
    elif row['riskband']=='CAT-D':
        if row['subrisk']=="Low":
            return 32.9
        elif row['subrisk']=="Med":
            return 32.9
        else:
            return 35.9
    else:
        return 0

def getPF(row):
    if row['riskband']=='CAT-A':
        if row['subrisk']=="Low":
            return 1.9
        elif row['subrisk']=="Med":
            return 1.9
        else:
            return 2.9
    elif row['riskband']=='CAT-B':
        if row['subrisk']=="Low":
            return 2.9
        elif row['subrisk']=="Med":
            return 3.9
        else:
            return 3.9
    elif row['riskband']=='CAT-C':
        if row['subrisk']=="Low":
            return 3.9
        elif row['subrisk']=="Med":
            return 4.9
        else:
            return 4.9
    elif row['riskband']=='CAT-D':
        if row['subrisk']=="Low":
            return 4.9
        elif row['subrisk']=="Med":
            return 5.9
        else:
            return 5.9
    else:
        return 0

def naps_func(data):
    df = data.copy()
    print(df.columns)
    df.rename(columns={'Secured Accounts Count': 'Secured_Accounts_Count',
    'Unsecured Accounts Count': 'Unsecured_Accounts_Count',
    'Secured High Credit Sum': 'Secured_High_Credit_Sum',
    'Unsecured High Credit Sum': 'Unsecured_High_Credit_Sum',
    'Secured Amount Overdue Sum': 'Secured_Amount_Overdue_Sum',
    'Unsecured Amount Overdue Sum': 'Unsecured_Amount_Overdue_Sum',
    'Secured Balances Sum': 'Secured_Balances_Sum',
    'Unsecured Balances Sum': 'Unsecured_Balances_Sum',
    'Own Accounts count': 'Own_Accounts_count',
    'Other Accounts count': 'Other_Accounts_count',
    'CIBILTUSC3 Score Value': 'CIBILTUSC3_Score_Value',
    'CIBILTUSC3 Score Reason Code Set': 'CIBILTUSC3_Score_Reason_Code_Set',
    'CIBILTUSC3 Score Exclusion Code Set': 'CIBILTUSC3_Score_Exclusion_Code_Set',
    'CIBILTUSC3 Score Error Code Set': 'CIBILTUSC3_Score_Error_Code_Set'},inplace=True)
    #df = pd.read_csv("F:/Niro/NAPS v3/Calibrations/FINBUD DS2 TLI.csv",low_memory=False)
    df.columns = [column.lower() for column in df.columns]
    
    df['odd_test'] = df['member reference']%2
    df = preprocessor(df)
    df['naps']= df.apply(getNapsScore,axis=1)
    df['salary']= df.apply(getIncome,axis=1)
    df['stated_income']= df.apply(getCxStatedIncome,axis=1)
    df['card_bkt']= df.apply(card_bkt,axis=1)
    
    df['emi']= df.apply(getEMI,axis=1)
    df['foir']=df['emi']/df['salary']
    df['freeIncome'] = df['salary']  - df['emi'] - 10000
    df['freeIncome'] = np.select(condlist=[((df['freeIncome']<=2000)&(df['naps']>=745)&(df['cibiltusc3_score_value']>=750))],\
                                choicelist=[5000],default=df['freeIncome'])

    df['decline'] = df.apply(apply_bureau,axis=1)
    df['incomeRules'] = df.apply(borderCases,axis=1)
    df['riskband'] = df.apply(getRiskBand,axis=1)
    df['subrisk'] = df.apply(getSubRiskBand,axis=1)
    df = lenderAssignment(df)

    df['finalTenor'] = df.apply(getMaxTenor,axis=1)
    df['maxEMI'] = np.minimum(df.apply(getMaxEMI,axis=1),df['freeIncome'])
    df['maxLoanAmount'] = np.minimum(df.apply(getMaxAmount,axis=1),df['maxEMI']*df['finalTenor'])
    df['finalEMI'] = 100*((df['maxLoanAmount']/(100*df['finalTenor'])).astype(int))
    df['finalROI'] = df.apply(getROI,axis=1)
    df['finalPF'] = df.apply(getPF,axis=1)

    df['decline_post'] = df.apply(apply_bureau_post,axis=1)

    df['pscore'] = pd.qcut(df['naps'],20,precision=2,duplicates='drop').astype(str)

    return df
# p = df.groupby(['pscore','card_bkt','decline','decline_post','riskband','subrisk','incomeRules',\
#                 'cibilv3_bkt']).agg({'member reference':'count','freeIncome':'mean','foir':'mean',\
#                                      'finalEMI':'mean','maxLoanAmount':'mean','finalTenor':'mean',\
#                                      'finalROI':'mean','finalPF':'mean'})

# p.reset_index(drop=False,inplace=True)

# p.to_csv('F:/Niro/NAPS v3/Calibrations/housingds6.csv')
#p.to_csv('F:/Niro/NAPS v3/Calibrations/finbudoct.csv')
