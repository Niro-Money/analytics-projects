import pandas as pd
import numpy as np
from __init__ import logger
path='dependencies/'
def PIN_check(df):
    ########---niro-pincode matching(first three digit should match)
    df.drop(['pin','City','statename'], axis=1, inplace=True)
    pin_org_data=pd.read_excel(path+'Master pincode_Niro-09Oct2023.xlsx',sheet_name='Master file')
    pin_org_data.drop_duplicates(subset='pincode',keep='first',inplace=True)
    logger.info(f"Operating in {len(pin_org_data)} pincodes")
    pin_org_data['Pin']=pin_org_data['pincode'].astype(str).str.strip().str[:3]#changing pincode size to first 3 digits to compare
    df['pin1_new']=df['pin1'].astype(str).str[:3] #changing pincode size to first 3 digits to compare
    df['pin2_new']=df['pin2'].astype(str).str[:3]
    df['pin3_new']=df['pin3'].astype(str).str[:3]
    df['pin4_new']=df['pin4'].astype(str).str[:3]
    df['pin5_new']=df['pin5'].astype(str).str[:3]
    df['pin6_new']=df['pin6'].astype(str).str[:3]

    #matching pincode and inserting value to 'pincode' column if pincode matched
    df['pincode_Approved']=np.select(condlist=[df.pin1_new.isin(pin_org_data.Pin)],
                                             choicelist=[df['pin1']])
    df['pincode_Approved']=np.select(condlist=[df.pin2_new.isin(pin_org_data.Pin)],
                                             choicelist=[df['pin2']],
                                             default=df['pincode_Approved'])
    df['pincode_Approved']=np.select(condlist=[df.pin3_new.isin(pin_org_data.Pin)],
                                             choicelist=[df['pin3']],
                                             default=df['pincode_Approved'])
    df['pincode_Approved']=np.select(condlist=[df.pin4_new.isin(pin_org_data.Pin)],
                                             choicelist=[df['pin4']],
                                             default=df['pincode_Approved'])
    df['pincode_Approved']=np.select(condlist=[df.pin5_new.isin(pin_org_data.Pin)],
                                             choicelist=[df['pin5']],
                                             default=df['pincode_Approved'])
    df['pincode_Approved']=np.select(condlist=[df.pin6_new.isin(pin_org_data.Pin)],
                                             choicelist=[df['pin6']],
                                             default=df['pincode_Approved'])
    
    if "qkr_pin" in df.columns:
       df['qkr_pin_new']=df['qkr_pin'].astype(str).str[:3]
       df['pincode_Approved']=np.select(condlist=[df.qkr_pin_new.isin(pin_org_data.Pin)],
                                             choicelist=[df['qkr_pin']],
                                             default=df['pincode_Approved'])
       df.drop(['qkr_pin_new'],axis = 1,inplace = True)
    else:
        pass
    
    df.drop(['pin1_new','pin2_new','pin3_new','pin4_new','pin5_new','pin6_new'],axis = 1,inplace = True)

    #################################################Adding city state to aaproved pin
    df8=pd.read_csv(path+'PAN India PIN Code master.csv',low_memory=False)
    df8=df8[['Tier ','City','statename']]
    df8.drop_duplicates(subset='Tier ',keep='first',inplace=True)
    df['pincode_Approved'].replace(0, np.NaN, inplace=True)
    df['pincode_Approved_3_digit']=df['pincode_Approved'].astype(str).str.strip().str[:3]
    df8.rename(columns={'Tier ':'pincode_Approved_3_digit'},inplace=True)
    df8['pincode_Approved_3_digit']=df8['pincode_Approved_3_digit'].astype(str).str.strip().str[:3]
    df = df.merge(df8, on='pincode_Approved_3_digit', how='left')
    df.drop(columns={'pincode_Approved_3_digit'},inplace=True)
    df['pincode_Approved'].fillna(0,inplace=True)
    df['pincode_Approved']=np.select(condlist=[(df['pincode_Approved']==0),(df['pincode_Approved']==999999),(df['pincode_Approved']==999998),(df['pincode_Approved'].isna()==True)],
                            choicelist=[110001,110001,110001,110001],
                            default=df['pincode_Approved'])    
    #############################checking PayU/Muthoot pincode
    df_pin=pd.read_excel(path+'Master pincode_Niro-09Oct2023.xlsx',sheet_name='Master file')
    df_PayU_pin=df_pin[df_pin['Payu']=='Y']
    df_Muthoot_pin=df_pin[df_pin['Muthoot']=='Y']
    df_LL_pin=df_pin[df_pin['Liquiloans']=='Y']
    df_PayU_pin=df_PayU_pin[['pincode']]
    df_Muthoot_pin=df_Muthoot_pin[['pincode']]
    df_LL_pin=df_LL_pin[['pincode']]
    df_PayU_pin['Pin']=df_PayU_pin['pincode'].astype(str).str.strip().str[:3]
    df_Muthoot_pin['Pin']=df_Muthoot_pin['pincode'].astype(str).str.strip().str[:3]
    df_LL_pin['Pin']=df_LL_pin['pincode'].astype(str).str.strip().str[:3]

    df['pincode_Approved_new']=df['pincode_Approved'].astype(str).str[:3]
   
    df['PayU_serviceable']=np.select(condlist=[(df['pincode_Approved']!='0')&(df['pincode_Approved_new'].isin(df_PayU_pin['Pin']))],      
                                     choicelist=["yes"],
                                     default="no")
    df['Muthoot_serviceable']=np.select(condlist=[(df['pincode_Approved']!='0')&(df['pincode_Approved_new'].isin(df_Muthoot_pin['Pin']))],      
                                     choicelist=["yes"],
                                         default="no") 
    df['LL_serviceable']=np.select(condlist=[(df['pincode_Approved']!='0')&(df['pincode_Approved_new'].isin(df_LL_pin['Pin']))],      
                                     choicelist=["yes"],
                                         default="no")    
    df.drop(['pincode_Approved_new'],axis = 1,inplace = True)
    
    return df

