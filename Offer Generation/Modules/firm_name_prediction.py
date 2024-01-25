import pandas as pd
import numpy as np
from __init__ import logger
import re
path='dependencies/'

def addressCleaner(data: pd.DataFrame) -> pd.DataFrame:
    logger.info("Processing Addresses...")
    temp = data[[i for i in data.columns if re.search(r"^addr", str(i))]]
    for col in temp.columns:
        temp[col] = temp[col].astype(str).str.replace(r"[,.:;]+", " ")
        temp[col] = temp[col].astype(str).str.replace(r"B'|'", " ")
        temp[col] = temp[col].astype(str).str.replace(r" +", " ")
        data[col] = temp[col]
    return data

def firm_name_prediction(df):
    logger.info(f"predicting firm name for {df.shape[0]} records")
    df['email_final']=df['email1'].fillna(df['email2'].fillna(df['email3'].fillna(df['email4'].fillna(df['email5'].fillna(df['email6'].fillna('noemail@noemail.com'))))))
    df = addressCleaner(df)
    return df


