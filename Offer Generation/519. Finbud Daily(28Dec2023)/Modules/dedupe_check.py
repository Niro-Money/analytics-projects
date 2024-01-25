import pandas as pd
import numpy as np
import os
from Modules.prod_connection import extractorFromQuery
# from Modules.prod_connection import prod_cl_data
from __init__ import logger
import hashlib
def hash_phone_number_base64(phone_number):
    hashed = hashlib.sha256(phone_number.encode('utf-8')).hexdigest()
    return hashed

def dedupe_checker(data):
    data['hashed_phone'] = list(map(lambda x : hash_phone_number_base64(x),data['Mobile_No']))
    data['hashed_phone'] = data['hashed_phone'].astype(str)
    
    path = "dependencies/offered/"
    
    if 'Rescrub' not in list(data['Platform_Partner_Name']):

        offers = """select o.hashed_phone from offers as o inner join loan_application la on la.offer_id = o.id where  la.loan_application_status not in ('UNCLAIMED','MOBILE_VERIFICATION','PAN_VERIFICATION') and la.pan_verification_status not in ('COMPLETED');"""
        users = """select u.hashed_phone from users as u left join offers as o on u.user_id=o.user_id left join  loan_application la on la.offer_id = o.id where la.loan_application_status not in ('UNCLAIMED','MOBILE_VERIFICATION','PAN_VERIFICATION') and la.pan_verification_status not in ('COMPLETED');"""
        cl_offers = """select phone_number from credit_line.cl_offers where is_deleted = False;"""
        # sharechat="""select o.hashed_phone from offers o inner join vendors v on o.vendor_uid=v.id where v.name in ('sharechat') and o.is_deleted=False;"""
        #pre_offer = pd.read_csv(path+'pre_offer_data.csv')
        #pre_offer.rename(columns={'Mobile_No':'hashed_phone'},inplace=True)
        pl_extractor = extractorFromQuery('PL')
        cl_extractor = extractorFromQuery('CL')
        offers = pl_extractor.queryExecutor(offers)
        users = pl_extractor.queryExecutor(users)
        cl_offers = cl_extractor.queryExecutor(cl_offers)
        # sharechat = pl_extractor.queryExecutor(sharechat)

        cl_offers['hashed_phone'] = cl_offers['phone_number'].astype(float).astype('int64').astype(str).str[-10:].apply(hash_phone_number_base64)
        offers['hashed_phone'] = offers['hashed_phone'].astype(str)
        cl_offers['hashed_phone'] = cl_offers['hashed_phone'].astype(str)
        # sharechat['hashed_phone'] = sharechat['hashed_phone'].astype(str)
        #pre_offer['hashed_phone'] = pre_offer['hashed_phone'].astype(float).astype('int64').astype(str).str.strip()[-10:]

        offers = offers[['hashed_phone']]
        cl_offers = cl_offers[['hashed_phone']]
        #pre_offer = pre_offer[['hashed_phone']]

    
        if 'Niro' not in list(data['Platform_Partner_Name']):
            logger.info('Hope you updated User file, switching to partner mode')
            users['hashed_phone'] = users['hashed_phone'].astype(str)
            users = users[['hashed_phone']]
            req_data = pd.concat([offers,cl_offers, users], axis=0)
        else:
            logger.info('OU mode')
            req_data = pd.concat([offers, cl_offers], axis=0)
        
        req_data.drop_duplicates(subset='hashed_phone', keep='first')
        data = data[~data['hashed_phone'].isin(req_data['hashed_phone'])]
        data = data[~data['hashed_phone'].isin(req_data['hashed_phone'])]
        data.drop_duplicates(subset='hashed_phone', keep='first', inplace=True)

    else:
        logger.info('Rescrub Data, thus skipping dedupe')
        data.drop_duplicates(subset='hashed_phone', keep='first', inplace=True)

    return data

