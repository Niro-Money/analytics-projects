import pandas as pd

def Quikr_attributes_adder(df):
    path='cibil-data/PII/'
    #################Adding Qkr attributes from 19_1
    df_RE=pd.read_excel(path+'Qkr_DS45_Regular_Data_Attributes.xlsx',sheet_name='RE')
    df_Jobs=pd.read_excel(path+'Qkr_DS45_Regular_Data_Attributes.xlsx',sheet_name='Jobs')
    df_Cars_Bikes=pd.read_excel(path+'Qkr_DS45_Regular_Data_Attributes.xlsx',sheet_name='CnB')
    df_Goods=pd.read_excel(path+'Qkr_DS45_Regular_Data_Attributes.xlsx',sheet_name='Goods')
    df_Services=pd.read_excel(path+'Qkr_DS45_Regular_Data_Attributes.xlsx',sheet_name='Services')
    
    df_RE=df_RE[['poster_mobile','Name','email','city','property_for','property_type','bhk','min_price','max_price','localities']]
    df_RE.rename(columns={'poster_mobile':'Member Reference',
                          'Name':'qkr_name',
                          'email':'qkr_email',
                          'city':'qkr_city',
                          'property_for':'qkr_property_for',
                          'property_type':'qkr_property_type',
                          'bhk':'qkr_bhk',
                          'min_price':'qkr_min_price',
                          'max_price':'qkr_max_price',
                          'localities':'qkr_localities'},inplace=True)
    df_RE['qkr_sheet_name']='Real_estate'
    
    df_Jobs=df_Jobs[['Name','Phone','Email','City','job_role','education_qualification','experience']]
    df_Jobs.rename(columns={'Name':'qkr_name',
                            'Phone':'Member Reference',
                            'Email':'qkr_email',
                            'City':'qkr_city',
                            'job_role':'qkr_job_role',
                            'education_qualification':'qkr_education_qualification',
                            'experience':'qkr_experience'},inplace=True)
    df_Jobs['qkr_sheet_name']='Jobs'
    
    df_Cars_Bikes=df_Cars_Bikes[['mobile','email','user_name','city','sub_cat_name','registration_yr','price_min','price_max','brands','models']]
    df_Cars_Bikes.rename(columns={'mobile':'Member Reference',
                                  'email':'qkr_email',
                                  'user_name':'qkr_name',
                                  'city':'qkr_city',
                                  'sub_cat_name':'qkr_sub_cat_name',
                                  'registration_yr':'qkr_registration_yr',
                                  'price_min':'qkr_price_min',
                                  'price_max':'qkr_price_max',
                                  'brands':'qkr_brands',
                                  'models':'qkr_models'
                                  },inplace=True)
    df_Cars_Bikes['qkr_sheet_name']='Cars_Bikes'  
    
    df_Goods=df_Goods[['mobile','email_id','user_name','city','subcategory','prod_type','prod_condition','price_min','price_max']]
    df_Goods.rename(columns={'mobile':'Member Reference',
                              'email_id':'qkr_email',
                              'user_name':'qkr_name',
                              'city':'qkr_city',
                              'subcategory':'qkr_sub_cat_name',
                              'prod_type':'qkr_prod_type',
                              'prod_condition':'qkr_prod_condition',
                              'price_min':'qkr_price_min',
                              'price_max':'qkr_price_max'
                              },inplace=True)
    df_Goods['qkr_sheet_name']='Goods'
    
    df_Services=df_Services[['name','mobile','email','city_name','cat_name']]
    df_Services.rename(columns={'mobile':'Member Reference',
                                'email':'qkr_email',
                                'name':'qkr_name',
                                'city_name':'qkr_city',
                                'cat_name':'qkr_sub_cat_name'
                                },inplace=True)
    df_Services['qkr_sheet_name']='Services'
    
    df_qkr1 =pd.concat([df_RE,df_Jobs,df_Cars_Bikes,df_Goods,df_Services], axis=0)
    #################Adding Qkr attributes from 19_2
    # df_RE=pd.read_excel(path+'Qkr_DS_46_Regular_Data_Attributes.xlsx',sheet_name='RE')
    df_Jobs=pd.read_excel(path+'Qkr_DS46_Regular_Data_Attributes.xlsx',sheet_name='Jobs')
    # # df_Cars_Bikes=pd.read_excel(path+'Quikr_Attributes_DS_28_2.xlsx',sheet_name='CnB')
    # # df_Goods=pd.read_excel(path+'Quikr_Attributes_DS_28_2.xlsx',sheet_name='Goods')
    # # # df_Services=pd.read_excel(path+'Quikr_Attributes_DS_28_2.xlsx',sheet_name='Services')
    
    # df_RE=df_RE[['poster_mobile','Name','email','city','property_for','property_type','bhk','min_price','max_price','localities']]
    # df_RE.rename(columns={'poster_mobile':'Member Reference',
    #                       'Name':'qkr_name',
    #                       'email':'qkr_email',
    #                       'city':'qkr_city',
    #                       'property_for':'qkr_property_for',
    #                       'property_type':'qkr_property_type',
    #                       'bhk':'qkr_bhk',
    #                       'min_price':'qkr_min_price',
    #                       'max_price':'qkr_max_price',
    #                       'localities':'qkr_localities'},inplace=True)
    # df_RE['qkr_sheet_name']='Real_estate'     
    # ,'education_qualification'
    df_Jobs=df_Jobs[['Name','Phone','Email','City','job_role','education_qualification','experience']]
    df_Jobs.rename(columns={'Name':'qkr_name',
                            'Phone':'Member Reference',
                            'Email':'qkr_email',
                            'City':'qkr_city',
                            'job_role':'qkr_job_role',
                            'education_qualification':'qkr_education_qualification',
                            'experience':'qkr_experience'},inplace=True)
    df_Jobs['qkr_sheet_name']='Jobs'
    
    # df_Cars_Bikes=df_Cars_Bikes[['mobile','email','user_name','city','sub_cat_name','registration_yr','price_min','price_max','brands','models']]
    # df_Cars_Bikes.rename(columns={'mobile':'Member Reference',
    #                               'email':'qkr_email',
    #                               'user_name':'qkr_name',
    #                               'city':'qkr_city',
    #                               'sub_cat_name':'qkr_sub_cat_name',
    #                               'registration_yr':'qkr_registration_yr',
    #                               'price_min':'qkr_price_min',
    #                               'price_max':'qkr_price_max',
    #                               'brands':'qkr_brands',
    #                               'models':'qkr_models'
    #                               },inplace=True)
    # df_Cars_Bikes['qkr_sheet_name']='Cars_Bikes'

    # df_Goods=df_Goods[['mobile','email_id','user_name','city','subcategory','prod_type','prod_condition','price_min','price_max']]
    # df_Goods.rename(columns={'mobile':'Member Reference',
    #                           'email_id':'qkr_email',
    #                           'user_name':'qkr_name',
    #                           'city':'qkr_city',
    #                           'subcategory':'qkr_sub_cat_name',
    #                           'prod_type':'qkr_prod_type',
    #                           'prod_condition':'qkr_prod_condition',
    #                           'price_min':'qkr_price_min',
    #                           'price_max':'qkr_price_max'
    #                           },inplace=True)
    # df_Goods['qkr_sheet_name']='Goods'

    # df_Services=df_Services[['name','mobile','email','city_name','cat_name']]
    # df_Services.rename(columns={'mobile':'Member Reference',
    #                             'email':'qkr_email',
    #                             'name':'qkr_name',
    #                             'city_name':'qkr_city',
    #                             'cat_name':'qkr_sub_cat_name'
    #                             },inplace=True)
    # df_Services['qkr_sheet_name']='Services'      
    
    df_qkr2=pd.concat([df_Jobs], axis=0)

    ###############################Merging Qkr attributes with cibil data
    df_qkr = pd.DataFrame()
    df_qkr=pd.concat([df_qkr1,df_qkr2], axis = 0)
    df_qkr=df_qkr[df_qkr['Member Reference'].isna()==False]
    df_qkr.drop_duplicates(subset = 'Member Reference',keep='first',inplace=True)
    df.drop_duplicates(subset = 'Member Reference',keep='first',inplace=True)
    df=df.merge(df_qkr,on='Member Reference',how='left')
    ##################################################################mapping 'qkr_city' to 'qkr_pin'
    df_pinmaster=pd.read_csv('dependencies/PAN India PIN Code master.csv')
    df_pinmaster=df_pinmaster[['City','pincode']]
    df_pinmaster.rename(columns={'pincode':'qkr_pin','City':'qkr_city'},inplace=True)
    df_pinmaster.drop_duplicates(subset='qkr_city', keep = 'first', inplace = True)
    df=df.merge(df_pinmaster, on='qkr_city', how='left')
    return df
       
