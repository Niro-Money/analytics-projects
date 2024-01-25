# -*- coding: utf-8 -*-
"""
Created on Mon Sep 12 2022

@author: Abhinav Gavireddi
"""

import pandas as pd
import numpy as np
import pickle
from itertools import cycle
from __init__ import logger

def levenshteinDistanceDP(row):
    '''Difference between two names qkr_name and name from bureau


    Parameters
    ----------
    row : pd.Series
        DESCRIPTION.
        calculates the distance between the two attributes.

    Returns
    -------
    integer
        DESCRIPTION.
        returns the number of edits that are required to make one attribute to another.

    '''

    token1 = str(row['qkr_name'])
    token2 = str(row['name'])
    len_token1 = len(token1)
    len_token2 = len(token2)

    if ((len_token1 <= 1) | (len_token2 <= 1)):
        return 99
    if ((token1 == "XXXX") | (token2 == "XXXX")):
        return 99

    distances = np.zeros((len_token1 + 1, len_token2 + 1))

    for t1 in range(len_token1 + 1):
        distances[t1][0] = t1

    for t2 in range(len_token2 + 1):
        distances[0][t2] = t2

    a = 0
    b = 0
    c = 0

    for t1 in range(1, len_token1 + 1):
        for t2 in range(1, len_token2 + 1):
            if (token1[t1-1] == token2[t2-1]):
                distances[t1][t2] = distances[t1 - 1][t2 - 1]
            else:
                a = distances[t1][t2 - 1]
                b = distances[t1 - 1][t2]
                c = distances[t1 - 1][t2 - 1]

                if (a <= b and a <= c):
                    distances[t1][t2] = a + 1
                elif (b <= a and b <= c):
                    distances[t1][t2] = b + 1
                else:
                    distances[t1][t2] = c + 1

    return distances[len_token1][len_token2]


def null_cleaner(data, k):
    '''removing null values and filling them with k


    Parameters
    ----------
    data : pd.DataFrame
    k : value to replace null values

    Returns
    -------
    data : pd.DataFrame
        DESCRIPTION.
        return DataFrame in which null values are replaced with k

    '''
    data = data.replace(-999, np.nan)
    data = data.replace(np.inf, np.nan)
    data = data.replace(-np.inf, np.nan)
    data = data.fillna(k)
    return data


def prop_cat(data, qkr):
    '''criteria 


    Parameters
    ----------
    data : pd.DataFrame
        DESCRIPTION.
        final Data which consists of the predicted propensity
    qkr : Boolean
        DESCRIPTION.
        for specifying whether the data is of quikr or other partner

    Returns
    -------
    data : pd.DataFrame
        DESCRIPTION.
        creates a column based on:
            case1: if qkr is True, conditionlist1 is cosidered for catergorization.
            case2: if qkr is False, conditionlist2 is considered for categorization.

    '''
    conditionlist1 = [data['new_prop'] > 0.45,
                      (data['new_prop'] <= 0.45) & (data['new_prop'] > 0.2)]
    conditionlist2 = [data['new_prop'] > 0.58,
                      (data['new_prop'] <= 0.58) & (data['new_prop'] > 0.38)]
    choicelist = ['HiiProp', 'MedProp']
    if qkr:
        conditionlist = conditionlist1
    else:
        conditionlist = conditionlist2
    data['propensity'] = np.select(condlist=conditionlist, choicelist=choicelist, default='LowProp')
    return data


def dummy_creator():
    '''dummys creator


    Returns
    -------
    dummy_rows : pd.DataFrame
        DESCRIPTION.
        return a DataFrame that consists of categorical variables

    '''
    ele = ['Cars_Bikes', 'Goods', 'Jobs', 'others', 'Real_estate', 'Services']
    bc = ['INACTIVE', 'INDTRM', 'NOBC', 'REVOLVER', 'RVLRPLUS', 'TRANPLUS', 'TRANSACTOR']
    ti = ['Tier_1', 'Tier_2', 'Tier_3']
    elements, bcpm, tier = cycle(ele), cycle(bc), cycle(ti)
    Range = range(max(len(bc), len(ele)))
    dummy_rows = pd.DataFrame()
    dummy_rows['qkr_sheet_name'] = [next(elements) for t in Range]
    dummy_rows['Tier'] = [next(tier) for t in Range]
    dummy_rows["BCPMTSTR"] = [next(bcpm) for t in Range]
    dummy_rows['qkr_name'] = "XXXX"
    return dummy_rows


def propensity_prediction(req_data):
    '''main function


    Parameters
    ----------
    req_data : pd.DataFrame
        DESCRIPTION.
        case1 : if the input DataFrame consits qkr_name and qkr_sheet_name, then model for qkr data is used.
        case2 : if the input DataFrame doesn't contain qkr_name and qkr_sheet_name, then model for general Data is used.

    Returns
    -------
    req_data : pd.DataFrame
        DESCRIPTION.
        return the DataFrame with predicted propensity column added to original DataFrame.

    '''
    dependency = 'dependencies/'
    pickle_loc = 'pickle/'
    cols = req_data.columns
    Tier_data = pd.read_csv(dependency+'Tier_Propensity.csv', low_memory=False)
    dummy_rows = dummy_creator()
    data = req_data.copy()
    data['Tier'] = np.select(
        condlist=[req_data['City'].isin(Tier_data['tier_1'].values), req_data['City'].isin(Tier_data['tier_2'].values)],
        choicelist=['Tier_1', 'Tier_2'],
        default='Tier_3')
    data = data.reset_index(drop=True)
    qkr = False
    if 'qkr_sheet_name' in list(data.columns) and 'qkr_name' in list(data.columns):
        qkr = True
        model = pickle.load(open(pickle_loc+"PropensityBalancedBaggedClassifier.pickle", 'rb'))
        logger.info("Using propensity model for Quikr Data")
        dummy_rows = dummy_rows.reset_index(drop=True)
        data = pd.concat([data, dummy_rows], axis='index', ignore_index=True)
        data = data.reset_index()
        data['pan_digits'] = data['PAN_NO_Checked'].astype(str).str.len()
        data['name'] = null_cleaner(data['name'], "XXXX")
        data['qkr_name'] = null_cleaner(data['qkr_name'], "XXXX")
        data['ed'] = data.apply(levenshteinDistanceDP, axis=1)
        data['len'] = (data['name'].str.len() + data['qkr_name'].str.len()) * 0.5
        data['dist'] = data['ed'] / data['len']
        data = null_cleaner(data, 0)
        X_pro = preprocessing(data)
        data['new_prop'] = model.predict_proba(X_pro)[:, 1]

    else:
        logger.info("Using General propensity model")
        model = pickle.load(open(pickle_loc+'BalancedRandomForestClassifier_final.pickle', 'rb'))
        data['new_prop'] = model.predict_proba(data)[:, 1]
    data['new_prop'] = data['new_prop'].apply(lambda x: round(x, 2))
    data = prop_cat(data, qkr)
    data = data[['Member Reference', 'new_prop', 'propensity']]
    req_data = req_data.merge(data, on='Member Reference', how='inner')
    req_data.rename(columns={'new_prop': 'pred_propensity'}, inplace=True)
    return req_data


def preprocessing(X):
    '''preprocessor


    Parameters
    ----------
    X : pd.DataFrame
        DESCRIPTION.
        converts categorical columns to binary columns by creating columns(m) equal to the number of 
        variables(n) in a category

    Returns
    -------
    X : pd.DataFrame
        DESCRIPTION.
        returns the processed DataFrame

    '''

    X = X[[
        "dist", "qkr_sheet_name", "Calculated_Age", "AGG911", "RVLR01", "BCPMTSTR", "CV11", "CV14", "MT28S", "MT33S", "PL33S", "AT20S",
        "MT01S", "BC02S", "BG01S", "CV10", "TRD", "AT33A", "AU33S", "AU28S", "PL28S", "BC28S", "G310S", "AGGS911", "AT01S", "AT33A_NE_CCOD",
        "CV14_12M", "CV14_6M", "CV14_3M", "CV14_1M", "G310S_24M", "G310S_6M", "G310S_3M", "G310S_1M", "G057S_1DPD_36M", "G057S_1DPD_12M",
        "BC106S_60DPD", "BC107S_24M", "BC106S_60DPD_12M", "BC107S_12M", "BC106S_LE_30DPD_12M", "BC09S_36M_HCSA_LE_30", "PL09S_36M_HCSA_LE_30", "AT09S_6M", "G310S_36M",
        "AT09S_12M", "AT09S_3M", "CV13", "CV24", "REVS904", "CV20", "CV22", "UL_TRD", "CV21", "G310S_2M", "Secured Accounts Count", "Unsecured Accounts Count",
        "Secured High Credit Sum", "Unsecured High Credit Sum", "Secured Amount Overdue Sum", "Unsecured Amount Overdue Sum", "Secured Balances Sum",
        "Unsecured Balances Sum", "Other Accounts count", "CIBILTUSC3 Score Value", "Tier"]]

    cols = X.columns
    num_cols = X._get_numeric_data().columns
    cat_col = list(set(cols) - set(num_cols))
    for i in list(X.select_dtypes(exclude='number').columns):
        idx = X[(X[i] == 0) | (X[i] == '0')].index
        X.loc[idx, i] = 'others'

    for i in cat_col:
        k = pd.get_dummies(X[i])
        try:
            k.drop('others', axis=1, inplace=True)
        except Exception:
            logger.info("no category name: others found in " + str(i))
        X = pd.concat([X, k], axis=1)

    X.drop(cat_col, axis=1, inplace=True)
    return X
