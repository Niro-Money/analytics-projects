import numpy as np
import pandas as pd
import pickle

pickleloc="pickle/"
path = "dependencies/"

def salary_prediction(df):
    df['pin1']=df['pin1'].astype(str).str.replace(' ','').str[:6]
    df['pin2']=df['pin2'].astype(str).str.replace(' ','').str[:6]
    df['pin3']=df['pin3'].astype(str).str.replace(' ','').str[:6]
    df['pin4']=df['pin4'].astype(str).str.replace(' ','').str[:6]
    df['pin5']=df['pin5'].astype(str).str.replace(' ','').str[:6]
    df['pin6']=df['pin6'].astype(str).str.replace(' ','').str[:6]
    

    df['pin1']=np.select( condlist=[df['pin1'].str.isdigit()==True],
                                                choicelist=[df['pin1']])
    df['pin1']=df['pin1'].astype(int)

    df['pin2']=np.select( condlist=[df['pin2'].str.isdigit()==True],
                                                choicelist=[df['pin2']])
    df['pin2']=df['pin2'].astype(int) 

    df['pin3']=np.select( condlist=[df['pin3'].str.isdigit()==True],
                                                choicelist=[df['pin3']])
    df['pin3']=df['pin3'].astype(int)

    df['pin4']=np.select( condlist=[df['pin4'].str.isdigit()==True],
                                                choicelist=[df['pin4']])
    df['pin4']=df['pin4'].astype(int)

    df['pin5']=np.select( condlist=[df['pin5'].str.isdigit()==True],
                                                choicelist=[df['pin5']])
    df['pin5']=df['pin5'].astype(int)

    df['pin6']=np.select( condlist=[df['pin6'].str.isdigit()==True],
                                                choicelist=[df['pin6']])
    df['pin6']=df['pin6'].astype(int)

    ###############pin 1-6 to one pin

    df['pin']=df['pin1']

    df['pin']=np.select( condlist=[df['pin']==0],
                                                choicelist=[df.pin2],
                              default=df.pin)
    df['pin']=np.select( condlist=[df['pin']==0],
                                                choicelist=[df.pin3],
                              default=df.pin)
    df['pin']=np.select( condlist=[df['pin']==0],
                                                choicelist=[df.pin4],
                              default=df.pin)
    df['pin']=np.select( condlist=[df['pin']==0],
                                                choicelist=[df.pin5],
                              default=df.pin)
    df['pin']=np.select( condlist=[df['pin']==0],
                                                choicelist=[df.pin6],
                              default=df.pin)

    #############filling pins having value 0
    df1=df[df['pin']!=0].head(len(df[df['pin']==0])) #choose the no same as "print(len(df[df['pin']==0]))" output
    df1.index=df[df['pin']==0].index #to copy values index must be same else nan(null) will be copied
    df.loc[df['pin']==0,'pin']=df1['pin']
    
    #############---Adding city state
    df2=pd.read_csv(path+"PAN India PIN Code master.csv",low_memory=False)
    df2=df2[['Tier ','City','statename']]
    df2['Tier ']=df2['Tier '].astype(str).str.strip().str[:3]
    df2.drop_duplicates(subset='Tier ', keep = 'first', inplace = True)
    df['pin3digit']=df['pin'].astype(str).str.strip().str[:3]
    df = df.merge(df2, left_on='pin3digit', right_on='Tier ', how='left')
    df.drop(columns={'pin3digit','Tier '},inplace=True)

    ################# scoring predicted income
    loaded_model = pickle.load(open(pickleloc+"salary_pred_model_cibil_data_v1.dat", "rb"))
    #final list of variables required for scoring
    xvars= ['Secured High Credit Sum','Unsecured High Credit Sum','pin_code_bucket','PL33S','BC28S','MT28S','PL28S','AT01S',
            'CV12','CV11','Unsecured Accounts Count','Other Accounts count','BC02S','AT20S','MT33S','Secured Accounts Count',
            'AU28S','CV10','BC09S_36M_HCSA_LE_30','AT09S_3M']

    df_pincode_bucket = pd.read_csv(path+"pin_code_bucket_180122.csv")
    df_pincode_bucket .drop(columns=['Avg_Salary'],axis=1,inplace=True)

    df_BCPMTSTR_bucket = pd.read_csv(path+"BCPMTSTR_bucket_180122.csv")
    df_BCPMTSTR_bucket.drop(columns=['Avg_Salary'],axis=1,inplace=True)

    df_sal_pred=pd.merge(df,df_pincode_bucket,on='pin',how='left')

    df_sal_pred=pd.merge(df_sal_pred,df_BCPMTSTR_bucket,on='BCPMTSTR',how='left')

    predicted_salary=loaded_model.predict(df_sal_pred[xvars])
    predicted_salary=pd.Series(predicted_salary)

    df=pd.concat([df,predicted_salary],axis=1)

    df=df.rename(columns={0:'pred_salary'})

    df['pred_salary']=df['pred_salary'].apply(np.int64)

    return df

