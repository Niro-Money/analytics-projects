import numpy as np

def PAN_check(df):
    df['PAN_NO_Checked']=df['PAN'].astype(str).str.replace(' ','')
    df['pan_digits']=df['PAN_NO_Checked'].astype(str).str.len()
    df['PAN_NO_Checked']=np.select(condlist=[(((df['PAN_NO_Checked'].str[3:4]=='P')|(df['PAN_NO_Checked'].str[3:4]=='p'))&(df['pan_digits']==10)),
                                                        (df['PAN_NO_Checked']=='nan')],
                                           choicelist=[df['PAN_NO_Checked'],df['PAN_NO_Checked']],
                                           default='wrong pan')
    return df



