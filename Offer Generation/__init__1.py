import logging
import os
import pickle
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import sys
import pandas as pd
import dotenv
dotenv.load_dotenv()

from categorizer import categorizer

logger = logging.getLogger("Offer_Generation")

root = Path(__file__).parent
logger.info(f"loading files from {root}")
logging.basicConfig(
    filename=root / "output.log",
    format="%(asctime)s -%(name)s -%(levelname)s- %(message)s",
    filemode="w",
)
formatter = logging.Formatter(
    "%(asctime)s  %(levelname)s - %(message)s" " [in %(pathname)s:%(lineno)d]"
)
file_handler = logging.FileHandler(root / "server.log", mode="a")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)
warnings.filterwarnings("ignore")

rule_engine_version = os.getenv('BRE_VERSION','rule_engine_v2')
naps_version = os.getenv('NAPS_VERSION','naps_model_v3')

root = Path(__file__).parent
dep_path = root / "dependencies"
pickle_path = root / "pickle"
tli_path = root / "cibil-data" / "TLI"
pii_path = root / "cibil-data" / "PII"
attr_path = root/"attributes"
if not os.path.exists(root / "Output"):
    os.mkdir(root / "Output")

save_path = root / "Output"
logger.info(f"loading TLI files from {tli_path}")
logger.info(f"loading PII files from {pii_path}")
logger.info(f"saving files in {save_path}")
logger.info(f"loading pickle files from {pickle_path}")
logger.info(f"loading dependencies from {dep_path}")
logger.info(f'using {rule_engine_version}')
logger.info(f'using {naps_version}')



# thirtyplus model parameters

date_today = pd.to_datetime(date.today())
rundate = f"{date_today.day}{date_today.strftime('%m')}{str(date_today.year)[-2:]}"

DATA_SOURCE = 'Quikr_Regular_DS_45&46'
OFFER_SOURCE = '383.Quikr_Regular_DS_45&46-'+rundate
PLATFORM_PARTNER = 'Quikr' 
DATA_PARTNER = 'Quikr'
Date='03-09-2023'
total_data_count = 1569832
Data_Tag='QKR_DS45&46_'
DD = '_03_SEP_2023'


CIBIL_T = 700
min_emi_T = 3000
min_amt_T = 50000
GST_perc = 18
NEW_FOIR_T = 70
DEC_AMT_T = 1000
muthoot_threshold = 0
muthoot_switchable_cats = ["CAT-D", "CAT-C"]

# lenders = {
#     "payU": {"maxAmount": 500000, "maxTenor": 36},
#     "muthoot": {"maxAmount": 220000, "maxTenor": 24},
#     "Liquiloans": {"maxAmount": 240000, "maxTenor": 24},
# }

# rateTestMap = defaultdict(
#     lambda: 0,
#     {
#         "Control": 0,
#         "Low_Rate_Test1": -2,
#         "Low_Rate_Test2": -2,
#         "HiAmt_LoBrdn_LoRate_Test2": -6,
#     },
# )

# tenorBurdenTestMap = defaultdict(
#     lambda: 0,
#     {
#         "Control": 0,
#         "Low_Burden_Test1": 3,
#         "Low_Burden_Test2": 6,
#         "HiAmt_LoBrdn_LoRate_Test2": 6,
#     },
# )

# amountTestMap = defaultdict(
#     lambda: 1,
#     {"Control": 1, "High_Amount_Test1": 1.3, "HiAmt_LoBrdn_LoRate_Test2": 1.3},
# )
# tenor_map = defaultdict(
#     lambda: 0,
#     {
#         "CAT-A": defaultdict(lambda: 0, {"L": 36, "M": 36, "H": 36}),
#         "CAT-B": defaultdict(lambda: 0, {"L": 36, "M": 36, "H": 24}),
#         "CAT-C": defaultdict(lambda: 0, {"L": 24, "M": 18, "H": 12}),
#         "CAT-D": defaultdict(lambda: 0, {"L": 12, "M": 12, "H": 6}),
#     },
# )
# tenorMax_map = defaultdict(lambda: 0, {"CAT-A": 36, "CAT-B": 36, "CAT-C": 24, "CAT-D": 18})
reg_pf_map = defaultdict(lambda: 10, {"CAT-A": 2.9, "CAT-B": 2.9, "CAT-C": 3.9, "CAT-D": 6.9})
fin_pf_map = defaultdict(lambda:10,{'CAT-A':2.9,'CAT-B':3.2,'CAT-C':4.2,'CAT-D':6.9})
# # rate_map = defaultdict(
#     lambda: 28,
#     {
#         "CAT-A": defaultdict(lambda: 28, {"L": 19.9, "M": 19.9, "H": 19.9}),
#         "CAT-B": defaultdict(lambda: 28, {"L": 21.9, "M": 22.9, "H": 23.9}),
#         "CAT-C": defaultdict(lambda: 28, {"L": 23.9, "M": 24.9, "H": 25.9}),
#         "CAT-D": defaultdict(lambda: 28, {"L": 24.9, "M": 25.9, "H": 26.9}),
#     },
# )
# maxcat2loan_map = defaultdict(
#     lambda: 0,
#     {
#         "CAT-A": defaultdict(lambda: 0, {"L": 500000, "M": 480000, "H": 450000}),
#         "CAT-B": defaultdict(lambda: 0, {"L": 480000, "M": 450000, "H": 400000}),
#         "CAT-C": defaultdict(lambda: 0, {"L": 240000, "M": 180000, "H": 120000}),
#         "CAT-D": defaultdict(lambda: 0, {"L": 56000, "M": 56000, "H": 56000}),
#     },
# )

tier = pd.read_csv(dep_path / "Tier_risk.csv")
thirtyplus_model = pickle.load(open(pickle_path / "risk_model_thirtyplus.pickle", "rb"))

# oneplus model parameters
oneplus_model = pickle.load(
    open(
        pickle_path / "risk_model.pickle",
        "rb",
    )
)
oneplus_scaler = pickle.load(
    open(
        pickle_path / "risk_scaler.pickle",
        "rb",
    )
)
cols = [
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
cols1 = [
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
cols2 = [
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
