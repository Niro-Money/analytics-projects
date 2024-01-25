import logging
import pickle
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from __init__ import *

warnings.filterwarnings("ignore")

path = Path(__file__).parent
logging.basicConfig(level=logging.INFO)


def preprocessor(data: pd.DataFrame) -> pd.DataFrame:
    data.select_dtypes(include="number").fillna(0, inplace=True)
    for i in data.select_dtypes(include="number").columns:
        if i in ["AGG911", "AGGS911"]:
            data[i] = np.select([data[i] > 100, data[i] < 0], [100, 0], data[i])
        else:
            data[i] = np.select([data[i] < 0], [0], default=data[i])

    data["gender"] = data["gender"].astype(str).str.lower()
    data.rename(
        columns={"Current_Salary": "salary", "pincode_Approved": "pincode"},
        errors="raise",
        inplace=True,
    )

    TIER.fillna("110", inplace=True)
    TIER["tier_1"] = TIER["tier_1"].astype(float).astype(int).astype(str).str.strip()
    TIER["tier_2"] = TIER["tier_2"].astype(float).astype(int).astype(str).str.strip()
    data["pincode"] = data["pincode"].astype(float).astype(int).astype(str).str.strip()
    tier_1 = list(set(TIER.tier_1.values))
    tier_2 = list(set(TIER.tier_2.values))
    data["tier"] = np.select(
        condlist=[data["pincode"].isin(tier_1), data["pincode"].isin(tier_2)],
        choicelist=["Tier_1", "Tier_2"],
        default="Tier_3",
    )
    return data


def bounce_preds(data: pd.DataFrame) -> pd.DataFrame:
    X = data.copy()
    clf = THIRTYPLUS_MODEL
    X = preprocessor(X)
    X["new_preds"] = clf.predict_proba(X)[:, 1]
    X["new_preds"] = np.select(
        condlist=[X["new_preds"] <= 0.000000000001, X["new_preds"] >= 0.99999999],
        choicelist=[0.000000000001, 0.999999999],
        default=X["new_preds"],
    )
    X["oddsratio"] = np.log(X["new_preds"] / (1 - X["new_preds"]))
    X["new_naps"] = (717.514 - 72.626 * X["oddsratio"]).astype("int64")
    X["naps_tu_thirtyplus"] = X["new_naps"].apply(lambda x: min(max(x, 350), 900))
    X["naps_tu_thirtyplus"] = list(map(lambda x: min(max(x, 350), 900), X["new_naps"]))
    return X["naps_tu_thirtyplus"]
