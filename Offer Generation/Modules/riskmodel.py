import numpy as np
import pandas as pd
from Modules.riskmodel_oneplus import predict
from Modules.riskmodel_thirtyplus import bounce_preds


def riskband1(data: pd.DataFrame) -> pd.Series:
    for i in [
        "CIBILTUSC3 Score Value",
        "AT20S",
        "TRD",
        "BC28S",
        "MT28S",
        "PL28S",
        "AU28S",
        "Unsecured High Credit Sum",
    ]:
        data[i].fillna(0, inplace=True)
        data[i] = data[i].astype(float)
    data["riskband"] = np.select(
        condlist=[
            (
                ((data["MT28S"] > 1000000) | (data["AU28S"] > 250000))
                & (data["BC28S"] > 100000)
                & (data["CIBILTUSC3 Score Value"] > 750)
                & (data["AT20S"] >= 48)
                & (data["naps_tu"] >= 750)
                & (data["naps_tu_thirtyplus"] >= 750)
            ),
            (
                (data["BC28S"] >= 10000)
                & (data["CIBILTUSC3 Score Value"] >= 730)
                & (data["AT20S"] >= 36)
                & (data["naps_tu"] >= 700)
                & (data["naps_tu_thirtyplus"] >= 700)
            )
            | (
                (data["CIBILTUSC3 Score Value"] >= 740)
                & ((data["MT28S"] >= 1000000) | (data["AU28S"] >= 500000))
                & (data["AT20S"] >= 36)
                & (data["naps_tu"] >= 720)
            ),
            (
                ((data["naps_tu"] >= 720) | (data["naps_tu_thirtyplus"] >= 720))
                & (data["CIBILTUSC3 Score Value"] >= 720)
                & (data["AT20S"] > 18)
                & ((data["BC28S"] >= 10000) | (data["Unsecured High Credit Sum"] >= 50000))
            ),
        ],
        choicelist=["CAT-A", "CAT-B", "CAT-C"],
        default="CAT-D",
    )
    return data["riskband"]


def riskband2(data: pd.DataFrame) -> pd.Series:
    data["riskband2"] = np.select(
        condlist=[
            (data["naps_tu_thirtyplus"] <= 690)
            | (data["naps_tu"] <= 690)
            | ((data["naps_tu_thirtyplus"] < 720) & (data["naps_tu"] <= 690)),
            ((data["riskband"] == "CAT-A") & ((data["naps_tu_thirtyplus"] < 800) | (data["naps_tu"] < 815))),
            ((data["riskband"] == "CAT-A") & ((data["naps_tu_thirtyplus"] >= 820) & (data["naps_tu"] >= 855))),
            ((data["riskband"] == "CAT-B") & ((data["naps_tu_thirtyplus"] < 760) | (data["naps_tu"] < 775))),
            ((data["riskband"] == "CAT-B") & ((data["naps_tu_thirtyplus"] >= 800) & (data["naps_tu"] >= 815))),
            ((data["riskband"] == "CAT-C") & ((data["naps_tu_thirtyplus"] <= 720) | (data["naps_tu"] < 735))),
            ((data["riskband"] == "CAT-C") & ((data["naps_tu_thirtyplus"] >= 740) & (data["naps_tu"] >= 755))),
            ((data["riskband"] == "CAT-D") & ((data["naps_tu_thirtyplus"] < 690) | (data["naps_tu"] < 630))),
            ((data["riskband"] == "CAT-D") & ((data["naps_tu_thirtyplus"] >= 695) & (data["naps_tu"] >= 705))),
        ],
        choicelist=["JUNK", "H", "L", "H", "L", "H", "L", "H", "L"],
        default="M",
    )
    return data["riskband2"]


def predictRisk(data: pd.DataFrame) -> pd.DataFrame:
    X = data.copy()
    X["naps_tu"] = predict(X)
    X["naps_tu_thirtyplus"] = bounce_preds(X)
    X["riskband"] = riskband1(X)
    X["riskband2"] = riskband2(X)
    return X[["riskband", "riskband2", "naps_tu", "naps_tu_thirtyplus"]]
