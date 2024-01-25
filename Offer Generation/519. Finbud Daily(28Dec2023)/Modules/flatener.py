import numpy as np
def flatener(df):
    df['ID_Type'].fillna(0,inplace=True)
    df['ID_Type']=np.select(condlist=[(df['ID_Type'].isna()),(df['ID_Type']==' ')],
                            choicelist=[0,0],
                            default=df['ID_Type'])
    df['ID_Type']=df['ID_Type'].astype(int)

    df_a = df[df['ID_Type']==1]
    df_b = df[df['ID_Type']==2]
    df_c = df[df['ID_Type']==3]
    df_d = df[df['ID_Type']==4]
    df_e = df[df['ID_Type']==5]
    #df_f = df[df['ID_Type']==6]
    
    ##########################change
    df_f = df[df['ID_Type']==0]
    df_f.drop_duplicates(subset='Member Reference', keep='first', inplace=True)
    ####################################
    
    df_a.rename(columns={'ID_Number':'PAN','Telephone':'Tele1','email_id':'email1','address':'addr1','PINCode':'pin1'\
                          ,'DateReported_address':'addrdate1'},inplace=True)
    df_a = df_a[['Member Reference','PAN','Tele1','email1','addr1','pin1','addrdate1']]

    df_b.rename(columns={'ID_Number':'Passport','Telephone':'Tele2','email_id':'email2','address':'addr2','PINCode':'pin2'\
                          ,'DateReported_address':'addrdate2'},inplace=True)
    df_b = df_b[['Member Reference','Passport','Tele2','email2','addr2','pin2','addrdate2']]

    df_c.rename(columns={'ID_Number':'id1','Telephone':'Tele3','email_id':'email3','address':'addr3','PINCode':'pin3'\
                          ,'DateReported_address':'addrdate3'},inplace=True)
    df_c = df_c[['Member Reference','id1','Tele3','email3','addr3','pin3','addrdate3']]

    df_d.rename(columns={'ID_Number':'id2','Telephone':'Tele4','email_id':'email4','address':'addr4','PINCode':'pin4'\
                          ,'DateReported_address':'addrdate4'},inplace=True)
    df_d = df_d[['Member Reference','id2','Tele4','email4','addr4','pin4','addrdate4']]

    df_e.rename(columns={'ID_Number':'id3','Telephone':'Tele5','email_id':'email5','address':'addr5','PINCode':'pin5'\
                          ,'DateReported_address':'addrdate5'},inplace=True)
    df_e = df_e[['Member Reference','id3','Tele5','email5','addr5','pin5','addrdate5']]

    df_f.rename(columns={'ID_Number':'id4','Telephone':'Tele6','email_id':'email6','address':'addr6','PINCode':'pin6'\
                          ,'DateReported_address':'addrdate6'},inplace=True)
    df_f = df_f[['Member Reference','id4','Tele6','email6','addr6','pin6','addrdate6']]

    df_all = df[['Member Reference','EnquiryControlNumber','name','DateofBirth','gender']]
    df_all.drop_duplicates(subset=['Member Reference'],inplace=True)
    

    df_all = df_all.merge(df_a,on='Member Reference',how='outer')
    df_all = df_all.merge(df_b,on='Member Reference',how='outer')
    df_all = df_all.merge(df_c,on='Member Reference',how='outer')
    df_all = df_all.merge(df_d,on='Member Reference',how='outer')
    df_all = df_all.merge(df_e,on='Member Reference',how='outer')
    df_all = df_all.merge(df_f,on='Member Reference',how='outer')
    
    return df_all

