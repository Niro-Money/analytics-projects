import pandas as pd
import numpy as np
from pathlib import Path
import statsmodels.api as sm
from __init__ import *


def preprocessor(data: pd.DataFrame) -> pd.DataFrame:
    # creating categorical variables from the ordinal variables
    data["BCPMTSTR"] = np.select(
        condlist=[
            (data["BCPMTSTR"] == "RVLRPLUS") | ((data["BCPMTSTR"] == "REVOLVER")),
            (data["BCPMTSTR"] == "TRANSACTOR") | ((data["BCPMTSTR"] == "TRANPLUS")),
        ],
        choicelist=["REVOLVER", "TRANSACTOR"],
        default=data["BCPMTSTR"],
    )
    data["AGGS911"] = data["AGGS911"].astype(int)
    data["AGGS911"] = np.select(
        condlist=[data["AGGS911"] > 95],
        choicelist=["High_Utilization"],
        default="Low_utilization",
    )
    data["BC28S"] = np.select(
        condlist=[data["BC28S"] > 200000],
        choicelist=["High_card_limit"],
        default="low_card_limit",
    )
    for i in COLS1:
        data[i] = np.select(condlist=[data[i] < 0], choicelist=[0], default=data[i])
    for i in ["G310S", "G310S_24M", "G310S_6M", "G310S_3M", "G310S_1M", "G310S_36M"]:
        data[i] = np.select(
            condlist=[data[i] == 1.5, data[i] >= 2],
            choicelist=["mid", "high"],
            default="low",
        )

    # creating dummies for the above variables
    dummy = pd.DataFrame()
    for i in data.select_dtypes(exclude="number").columns:
        temp = pd.get_dummies(data[i])
        cols = dict([(j, str(i) + "_" + str(j)) for j in temp.columns])
        temp.rename(columns=cols, inplace=True)
        dummy = pd.concat([dummy, temp], axis=1)
    data = pd.concat([data, dummy], axis=1)

    X_pro = pd.DataFrame(
        data=ONEPLUS_SCALER.transform(data[list(ONEPLUS_SCALER.feature_names_in_)]),
        columns=list(ONEPLUS_SCALER.feature_names_in_),
        index=data.index,
    )
    for i in X_pro.columns:
        data[i] = X_pro[i]

    for i in COLS2:
        if i not in list(data.columns):
            data[i] = 0
    return data


def predict(X: pd.DataFrame) -> pd.Series:
    """Return the risk score in range of 300 - 900 based on the CV attributes as features and
        underlying model as logistic Regression

    Args:
        X (DataFrame)

    Returns:
        DataFrame: contains the predicted score for each record
    """
    data = X[COLS]
    data = preprocessor(data)
    # prediction
    data = sm.add_constant(data[COLS2].astype(float), has_constant="add")
    data["preds"] = ONEPLUS_MODEL.predict(data[list(ONEPLUS_MODEL.params.index)])
    data["updated_preds"] = np.select(
        condlist=[data["preds"] <= 0.000000000001, data["preds"] >= 0.99999999],
        choicelist=[0.000000000001, 0.999999999],
        default=data["preds"],
    )
    data["oddsratio"] = np.log(data["updated_preds"] / (1 - data["updated_preds"]))
    data["naps"] = (646.015 - 120.69 * data["oddsratio"]).astype("int64")
    # data["naps_tu"] = data["naps"].apply(lambda x: min(max(x, 350), 900))
    data["naps_tu"] = list(map(lambda x: min(max(x, 350), 900), data["naps"]))
    return data["naps_tu"]
