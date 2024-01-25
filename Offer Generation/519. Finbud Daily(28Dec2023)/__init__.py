import os
import pickle
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import logging
from pathlib import Path
import sys
import pandas as pd
from dotenv import load_dotenv
from categorizer import categorizer
import warnings
warnings.filterwarnings("ignore")
load_dotenv()
ROOT = Path(__file__).parent

def get_logger():
    logger = logging.getLogger("Offer_Generation")
    formatter = logging.Formatter("%(asctime)s  %(levelname)s - %(message)s [in %(pathname)s:%(lineno)d]")

    # File handler for server.log
    file_handler1 = logging.FileHandler(ROOT / "server.log", mode="a")
    file_handler1.setLevel(logging.DEBUG)  # Set to DEBUG to log everything
    file_handler1.setFormatter(formatter)
    logger.addHandler(file_handler1)

    # File handler for output.log
    file_handler2 = logging.FileHandler(ROOT / "output.log", mode="w")  # Overwrite the file every time
    file_handler2.setLevel(logging.INFO)  # Set to INFO to log only INFO and above
    file_handler2.setFormatter(formatter)
    logger.addHandler(file_handler2)
    return logger


RULE_ENGINE_VERSION = os.getenv("BRE_VERSION", "rule_engine_v4")
NAPS_VERSION = os.getenv("NAPS_VERSION", "naps_model_v3")

logger = get_logger()
DEP_PATH = ROOT / "dependencies"
PICKLE_PATH = ROOT / "pickle"
TLI_PATH = ROOT / "cibil-data" / "TLI"
PII_PATH = ROOT / "cibil-data" / "PII"
ATTR_PATH = ROOT / "attributes"

if not os.path.exists(ROOT / "Output"):
    os.mkdir(ROOT / "Output")
SAVE_PATH = ROOT / "Output"

# thirtyplus model parameters
date_today = pd.to_datetime(date.today())
RUNDATE = f"{date_today.day}{date_today.strftime('%m')}{str(date_today.year)[-2:]}"

DATA_SOURCE = "Finbud Daily 28_dec_2023"
OFFER_SOURCE = "519. Finbud Daily 28_dec 2023 " + RUNDATE
PLATFORM_PARTNER = "Finbud"
DATA_PARTNER = "Finbud"
DATE = str(date_today)
TOTAL_COUNT =5000
DATA_TAG = "FB_Daily_"
DD = "_30_DEC_2023"

CIBIL_T = 700
MIN_EMI_T = 3000
MIN_AMT_T = 50000
GST_PERC = 18
NEW_FOIR_T = 70
DEC_AMT_T = 1000
MUTHOOT_T = 0.3
MUTHOOT_CATS = ["CAT-D", "CAT-C"]
APPROVED = "NOT DECLINED"

# basic path info
logger.info(f"loading files from {ROOT}")
logger.info(f"loading TLI files from {TLI_PATH}")
logger.info(f"loading PII files from {PII_PATH}")
logger.info(f"saving files in {SAVE_PATH}")
logger.info(f"loading pickle files from {PICKLE_PATH}")
logger.info(f"loading dependencies from {DEP_PATH}")
logger.info(f"using {RULE_ENGINE_VERSION}")
logger.info(f"using {NAPS_VERSION}")
logger.info(f"creating offers for {DATA_SOURCE}")
logger.info(f"creating offers for {PLATFORM_PARTNER}")
logger.info(f"Total count of data {TOTAL_COUNT}")

# lender characteristics
LENDERS = {
    "PayU": {"maxAmount": 500000, "maxTenor": 36},
    # "Muthoot": {"maxAmount": 250000, "maxTenor": 24},
    "Liquiloans": {"maxAmount": 240000, "maxTenor": 24},
}

RATE_TEST_MAP = defaultdict(
    lambda: 0,
    {
        "Control": 0,
        "Low_Rate_Test1": -2,
        "Low_Rate_Test2": -2,
        "HiAmt_LoBrdn_LoRate_Test2": -6,
    },
)

TENOR_BUREDEN_TEST_MAP = defaultdict(
    lambda: 0,
    {
        "Control": 0,
        "Low_Burden_Test1": 3,
        "Low_Burden_Test2": 6,
        "HiAmt_LoBrdn_LoRate_Test2": 6,
    },
)

AMOUNT_TEST_MAP = defaultdict(
    lambda: 1,
    {"Control": 1, "High_Amount_Test1": 1.3, "HiAmt_LoBrdn_LoRate_Test2": 1.3},
)

FREE_INCOME_MULT = defaultdict(
    lambda: 0,
    {
        "CAT-A": defaultdict(lambda: 0, {"L": 1, "M": 1, "H": 0.8}),
        "CAT-B": defaultdict(lambda: 0, {"L": 1, "M": 1, "H": 0.7}),
        "CAT-C": defaultdict(lambda: 0, {"L": 1, "M": 1, "H": 0.6}),
        "CAT-D": defaultdict(lambda: 0, {"L": 1, "M": 0.8, "H": 0.7}),
    },
)

TENOR_MAP = defaultdict(lambda: 0, {"CAT-A": 36, "CAT-B": 36, "CAT-C": 18, "CAT-D": 12})
TENOR_MAX_MAP = defaultdict(lambda: 0, {"CAT-A": 36, "CAT-B": 36, "CAT-C": 24, "CAT-D": 18})
REG_PF_MAP = defaultdict(lambda: 10, {"CAT-A": 2.9, "CAT-B": 2.9, "CAT-C": 3.9, "CAT-D": 6.9})
FIN_PF_MAP = defaultdict(lambda: 10, {"CAT-A": 2.9, "CAT-B": 3.2, "CAT-C": 4.2, "CAT-D": 6.9})
PF_MAP = REG_PF_MAP if DATA_PARTNER != "Finbud" else FIN_PF_MAP
RATE_MAP = rate_map = defaultdict(
    lambda: 28,
    {
        "CAT-A": defaultdict(lambda: 28, {"L": 18.9, "M": 18.9, "H": 18.9}),
        "CAT-B": defaultdict(lambda: 28, {"L": 21.9, "M": 21.9, "H": 22.9}),
        "CAT-C": defaultdict(lambda: 28, {"L": 23.9, "M": 23.9, "H": 23.9}),
        "CAT-D": defaultdict(lambda: 28, {"L": 27.9, "M": 27.9, "H": 27.9}),
    },
    )



RATE_MULT_MAP = defaultdict(lambda:0,{'H':0,'M':0,'L':0})
MAX_CAT_LOAN_MAP = defaultdict(lambda:0,{'CAT-A':500000,'CAT-B':400000,'CAT-C':180000,'CAT-D':62000})
MAX_CAT2_LOAN_MULT = defaultdict(lambda:0,{'L':1,'M':0.9,'H':0.8})


PROD_SQL_USERNAME = os.getenv('PROD_SQL_USERNAME',None)
PROD_SQL_HOSTNAME = os.getenv('PROD_SQL_HOSTNAME',None)
PROD_SQL_PASSWORD = os.getenv('PROD_SQL_PASSWORD',None)
PROD_SQL_MAIN_DATABASE = os.getenv('PROD_SQL_MAIN_DATABASE',None)

CL_SQL_USERNAME = os.getenv('CL_SQL_USERNAME',None)
CL_SQL_HOSTNAME = os.getenv('CL_SQL_HOSTNAME',None)
CL_SQL_PASSWORD = os.getenv('CL_SQL_PASSWORD',None)
CL_SQL_MAIN_DATABASE = os.getenv('CL_SQL_MAIN_DATABASE',None)

SQL_PORT = int(os.getenv('SQL_PORT',None))
SSH_HOST = os.getenv('SSH_HOST',None)
SSH_USER = os.getenv('SSH_USER',None)
SSH_PORT = int(os.getenv('SSH_PORT',None))
LOCAL_HOST = os.getenv('LOCAL_HOST',None)

TIER = pd.read_csv(DEP_PATH / "Tier_risk.csv")
THIRTYPLUS_MODEL = pickle.load(open(PICKLE_PATH / "risk_model_thirtyplus.pickle", "rb"))

# oneplus model parameters
ONEPLUS_MODEL = pickle.load(
    open(
        PICKLE_PATH / "risk_model.pickle",
        "rb",
    )
)
ONEPLUS_SCALER = pickle.load(
    open(
        PICKLE_PATH / "risk_scaler.pickle",
        "rb",
    )
)
COLS = [
    "AGG911",
    "RVLR01",
    "BCPMTSTR",
    "CV11",
    "CV14",
    "MT28S",
    "MT33S",
    "PL33S",
    "AT20S",
    "MT01S",
    "BC02S",
    "BG01S",
    "CV10",
    "TRD",
    "AT33A",
    "AU33S",
    "CO04S180",
    "AU28S",
    "PL28S",
    "CO01S180",
    "BC28S",
    "CV12",
    "CO05S",
    "G310S",
    "AGGS911",
    "AT01S",
    "AT33A_NE_CCOD",
    "CV14_12M",
    "CV14_6M",
    "CV14_3M",
    "CV14_1M",
    "G310S_24M",
    "G310S_6M",
    "G310S_3M",
    "G310S_1M",
    "CV11_24M",
    "CV11_12M",
    "G057S_1DPD_36M",
    "G057S_1DPD_12M",
    "BC106S_60DPD",
    "BC107S_24M",
    "BC106S_60DPD_12M",
    "BC107S_12M",
    "BC106S_LE_30DPD_12M",
    "BC09S_36M_HCSA_LE_30",
    "PL09S_36M_HCSA_LE_30",
    "AT09S_6M",
    "G310S_36M",
    "AT33A_NE_WO",
    "AT09S_12M",
    "AT09S_3M",
    "Secured Accounts Count",
    "Unsecured Accounts Count",
    "Secured High Credit Sum",
    "Unsecured High Credit Sum",
    "Secured Amount Overdue Sum",
    "Unsecured Amount Overdue Sum",
    "Secured Balances Sum",
    "Unsecured Balances Sum",
    "Other Accounts count",
    "Calculated_Age",
]  # features used for model
COLS1 = [
    "MT33S",
    "MT28S",
    "RVLR01",
    "AT33A_NE_WO",
    "PL28S",
    "CO04S180",
    "AGG911",
    "BC02S",
    "BC09S_36M_HCSA_LE_30",
    "BC106S_60DPD",
    "BC107S_24M",
    "BC106S_60DPD_12M",
    "BC107S_12M",
    "BC106S_LE_30DPD_12M",
    "PL09S_36M_HCSA_LE_30",
]
COLS2 = [
    "CV14",
    "PL28S",
    "AT01S",
    "CV14_12M",
    "CV14_3M",
    "CV11_24M",
    "Secured Accounts Count",
    "BCPMTSTR_NOBC",
    "BC28S_low_card_limit",
    "AGGS911_High_Utilization",
    "G310S_6M_high",
    "G310S_3M_mid",
    "G310S_36M_high",
]


def applyFilter(func) -> pd.DataFrame | pd.Series:
    def filteringWrapper(*args, **kwargs):
        data = args[0]
        if "dec_reason" in data.columns:
            approved = data[data["dec_reason"] == "NOT DECLINED"]
            idx = approved.index
            logger.info(f"Executing {func.__name__} on {approved.shape[0]} records")
        else:
            logger.info(f"Executing {func.__name__} on {data.shape[0]} records")
        res = func(*(approved,), **kwargs)
        if isinstance(res, str | bool):
            logger.info(f"{func.__name__} - {res}")
            return res
        elif isinstance(res, pd.DataFrame):
            if "dec_reason" in data.columns:
                logger.info(f"{func.__name__} - {res[res['dec_reason']=='NOT DECLINED'].shape[0]}")
            if "offer_amount" in res.columns:
                n = res[res["offer_amount"] < (MIN_AMT_T + DEC_AMT_T)].shape[0]
                logger.info(f"records having loan amount <{MIN_AMT_T+DEC_AMT_T} {n}")
            data.loc[idx, list(res.columns)] = res.loc[:, list(res.columns)]
            return data
        elif func.__name__ == "resultViewer":
            return None
        else:
            logger.info(f"processed on {res.shape[0]} records")
            return res

    return filteringWrapper


def validation(func) -> pd.DataFrame | None:
    def validationWrapper(*args, **kwargs):
        data = args[0]
        if not isinstance(data, pd.DataFrame):
            raise TypeError("expected Dataframe got something else")
        else:
            if func.__name__ == "decReasonStack":
                reason = args[1]
                for col in ["offer_amount", "max_emi", "New_FOIR"]:
                    if col not in data.columns:
                        logger.warning(f"creating a temp {col} column")
                        data[col] = 0
                return func(*(data, reason), **kwargs)
            else:
                res = func(*args, **kwargs)
                if isinstance(res,pd.DataFrame):
                    sample = res[res['dec_reason']=='NOT DECLINED']
                    if all(item in sample.columns for item in ['offer_amount', 'max_tenor']):
                        for name, attrs in LENDERS.items():
                            assert (
                                sample[sample["lending partner"] == name]["offer_amount"].max()
                                <= attrs["maxAmount"]
                            ), f"loan Amount policy mismatch for {name}"
                            assert (
                                sample[sample["lending partner"] == name]["max_tenor"].max()
                                <= attrs["maxTenor"]
                            ), f"Tenor policy mismatch for {name}"
                return res                    
    return validationWrapper
