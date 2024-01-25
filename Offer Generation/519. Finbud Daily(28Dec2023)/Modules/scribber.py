import pandas as pd
import numpy as np

df = pd.read_csv(r"C:\Users\T14s\Desktop\offers\383. Quikr Regular DS45&46\Output\383.Quikr_Regular_DS_45&46-40923.csv",low_memory=False,nrows = 50000)
x = df[df['dec_reason']=='FOIR>70% with New EMI']
sample = x.copy()


def policyAdapter(row):
    min_amt = 50000
    min_emi_amt = 3000
    tenors = [36,24,18,12]
    amt = row['SYSTEM_LOAN_OFFER']
    tenor = row['max_tenor']
    emi = row['SYSTEM_MAX_EMI']
    roi = row['SYSTEM_RATEOFINT']
    decReason = row['dec_reason']
    roi_m = roi/1200

    def emiCalculator(tenor):
        amort = (1 +  roi_m)**tenor
        emi = amt*(roi*amort)/(amort-1)
        return emi

    def calculator(tenor):
        if (amt>=50000)&(emi<3000)&(decReason!='NOT DECLINED'):
            for i in range(len(tenors)):
                if tenors[i]<tenor:
                    emi_cal = emiCalculator(tenors[i])
                    if emi_cal>min_emi_amt:
                        decReason2 = 'NOT DECLINED'
                        return calculator(tenor),emi_cal,decReason2
        else:
            return tenor,emi,decReason
    return calculator(tenor)

sample['temp'] = sample.apply(policyAdapter,axis=1)
sample[['SYSTEM_LOAN_OFFER','SYSTEM_MAX_EMI','max_tenor','dec_reason','temp']].to_csv(r'C:\Users\T14s\Desktop\offers\383. Quikr Regular DS45&46\foir_declined_cases.csv')