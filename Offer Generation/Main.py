##############################################################
#                 ███╗░░██╗██╗██████╗░░█████╗░               #
#                 ████╗░██║██║██╔══██╗██╔══██╗               #
#                 ██╔██╗██║██║██████╔╝██║░░██║               #
#                 ██║╚████║██║██╔══██╗██║░░██║               #
#                 ██║░╚███║██║██║░░██║╚█████╔╝               #
#                 ╚═╝░░╚══╝╚═╝╚═╝░░╚═╝░╚════╝░               #
##############################################################

from __init__ import logger,RULE_ENGINE_VERSION,NAPS_VERSION
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import numpy as np
import datetime as dt
from datetime import date
import collections
import random
import joblib
import math
import warnings
import os
from Modules.flatener import flatener
from Modules.age_calculator import age_calculator
from Modules.salary_prediction import salary_prediction
from Modules.propensity_prediction_13Sep2022 import propensity_prediction
from Modules.PAN_check import PAN_check
from Modules.PIN_check import PIN_check
from Modules.firm_name_prediction import firm_name_prediction
from Modules.name_cleaner import name_cleaner
from Modules.dedupe_check import dedupe_checker
from Modules.language_predictor import Nlanguage
from pathlib import Path
from Modules.Quikr_attributes_adder import Quikr_attributes_adder
from  Modules.output_creator import Output_Creator
from Modules.twowayvar import naps_func
warnings.filterwarnings('ignore')
path = Path(__file__).parent


def dependency_maker():
    '''file Creation/extraction


    Returns
    -------
    PII : pd.DataFrame
        DESCRIPTION.
    TLI : pd.DataFrame
        DESCRIPTION.
        For Finbud : creates PII and TLI files by concatinating
        For Quikr : Extracts PII and TLI 
    '''
    cv_path = path / 'cibil-data' /'TLI'
    pii_path = path  / 'cibil-data' /'PII'
    attr = path / 'attributes'
    PII, TLI,attributes = pd.DataFrame(), pd.DataFrame(),pd.DataFrame()
    for file in os.listdir(cv_path):
        temp = pd.read_csv(cv_path / file, low_memory=False)
        TLI = pd.concat([TLI, temp], axis=0)
    for file in os.listdir(pii_path):
        temp = pd.read_csv(pii_path / file, low_memory=False) 
        PII = pd.concat([PII, temp], axis=0)
    PII.rename(columns={'MemberReference':'Member Reference'},inplace=True)
    PII = flatener(PII)
    # for file in os.listdir(attr):
    #     try:
    #         temp = pd.read_excel(attr / file,sheet_name='Sheet1')
    #     except:
    #         logger.info(f"Excel sheet for attr not found, trying to read .csv file in {attr}")
    #         temp = pd.read_csv(attr/file,low_memory=False)
    #     attributes = pd.concat([attributes, temp], axis=0)
    # logger.info('Length of attributes file:',len(attributes))
    # attributes.dropna(subset='Telephone Number 1 (Mobile)',inplace=True)
    # attributes['Telephone Number 1 (Mobile)'] = attributes['Telephone Number 1 (Mobile)'].astype(float).astype('int64').astype(str).str[-10:]
    return PII, TLI, attributes

    


if __name__ == '__main__':

    PII, TLI, attr = dependency_maker()
    df1, df2 = PII.copy(), TLI.copy()
    logger.info(f"Length of TLI {len(df2)}")
    df2.drop_duplicates(subset='Member Reference', keep='first', inplace=True)
    logger.info(f"Length after droping duplicates {len(df2)}")
    
    logger.info(f"length of PII file {len(df1)}")
    logger.info(f"length of columns of PII file {len(df1.columns)}")
    df2 = df2[df2['Member Reference'].isin(df1['Member Reference'])]
    logger.info(f"length of tradeline file {len(df2)}")
    logger.info(f"length of columns of TLI file {len(df2.columns)}")
    
    CD = df2.merge(df1, on='Member Reference', how='left')
    logger.info(f"length of CD {len(CD)}")
    logger.info(f"length of columns of CD {len(CD.columns)}")
    
    # CD = CD.rename(columns={'Member Reference': 'Member Reference Partner'})
    # CD = CD.merge(attr, left_on='Member Reference Partner', right_on='Member Reference Number', how='left')
    # CD.rename(columns={'Telephone Number 1 (Mobile)': 'Member Reference'}, inplace=True)
    logger.info(f"length of null Member Reference: {CD['Member Reference'].isna().sum()}")
    CD['Member Reference'].fillna(0, inplace=True)
    logger.info(f'length of CD after attr {len(CD)}')
    CD.rename(columns={'gender_x':'gender'},inplace=True)
    #---Age calculation
    CD['DateofBirth']=CD['DateofBirth'].apply(pd.to_numeric, errors='coerce') 
    CD['DateofBirth'].fillna(0, inplace=True)
    CD['Calculated_Age'] = CD['DateofBirth'].apply(age_calculator)
    
    
    # ---PAN check
    CD = PAN_check(CD)
    
    # ---Salary prediction
    CD = salary_prediction(CD)
    
    # ---propensity prediction
    CD = propensity_prediction(CD)
    
    # ---Firm name prediction
    CD = firm_name_prediction(CD)
    
    # ---PIN check
    CD = PIN_check(CD)
    
    
    # ---name cleaning
    CD['Final_name'] = name_cleaner(CD['name'])
    
    # langugage
    # CD = Nlanguage(CD)
    CD.to_csv('pre_processed_file.csv',index=False)
    CD = pd.read_csv('pre_processed_file.csv')
    # ---Rule Engine
    CD = naps_func(CD)
    
    Output_Creator(CD)
