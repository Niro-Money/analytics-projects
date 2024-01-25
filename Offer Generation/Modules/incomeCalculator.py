import pandas as pd
import numpy as np


def availableIncomeEstimator(data):
    X = data.copy()
    X["transactor"] = np.select(
        condlist=[X["BCPMTSTR"] == "TRANPLUS", X["BCPMTSTR"] == "TRANSACTOR"],
        choicelist=[2, 1],
        default=0,
    )
    X["cc_other"] = np.select(
        condlist=[X["BCPMTSTR"] == "INDTRM", X["BCPMTSTR"] == "INACTIVE"],
        choicelist=[2, 1],
        default=0,
    )
    X["cardlim"] = np.select(
        condlist=[
            X["BC28S"] >= 500000,
            X["BC28S"] >= 300000,
            X["BC28S"] >= 150000,
            X["BC28S"] >= 100000,
            X["BC28S"] > 50000,
        ],
        choicelist=[6, 4, 2, 1, 0],
        default=-1,
    )

    X["open_auto_sanctions"] = X["AU28S"]
    X["open_auto_sanctions"][X["open_auto_sanctions"] <= 0] = 0
    X["open_auto_sanctions"].fillna(0, inplace=True)

    X["open_mortgage_sanctions"] = X["MT28S"]
    X["open_mortgage_sanctions"][X["open_mortgage_sanctions"] <= 0] = 0
    X["open_mortgage_sanctions"].fillna(0, inplace=True)

    X["open_personal_sanctions"] = X["PL28S"]
    X["open_personal_sanctions"][X["open_personal_sanctions"] <= 0] = 0
    X["open_personal_sanctions"].fillna(0, inplace=True)

    X["open_revolving_sanctions"] = X["BC28S"]
    X["open_revolving_sanctions"][X["open_revolving_sanctions"] <= 0] = 0
    X["open_revolving_sanctions"].fillna(0, inplace=True)

    X = X.copy()

    X["delq_ind"] = (X["CV10"] * 2) + (X["CV11"] * 3) + (X["CV12"] * 4) + (X["G310S_3M"] * 1)
    X["foir_dlq"] = np.select(
        condlist=[
            X["delq_ind"] >= 4,
            X["delq_ind"] >= 3,
            X["delq_ind"] >= 2,
            X["delq_ind"] >= 1,
        ],
        choicelist=[1.5, 1, 0.6, 0.5],
        default=0.4,
    )
    X["foir_cibil"] = np.select(
        condlist=[
            X["CIBILTUSC3 Score Value"] <= 10,
            X["CIBILTUSC3 Score Value"] < 680,
        ],
        choicelist=[
            0.5,
            (62.5 - 0.082 * X["CIBILTUSC3 Score Value"]).astype(int) * 0.1,
        ],
        default=(18.4 - 0.018 * X["CIBILTUSC3 Score Value"]).astype(int) * 0.1,
    )

    X["foir_trd"] = np.select(
        condlist=[
            ((X["MT28S"] > 1000000) & (X["AU28S"] > 600000)),
            (X["MT28S"] > 1000000),
            (X["AU28S"] > 600000),
            (X["MT28S"] > 200000),
            (X["AU28S"] > 300000),
            (X["BC28S"] > 100000),
            (X["BC28S"] > 20000),
        ],
        choicelist=[0.35, 0.36, 0.37, 0.39, 0.4, 0.43, 0.45],
        default=0.5,
    )

    X["foir_mean"] = (X["foir_dlq"] + X["foir_cibil"] + X["foir_trd"]) / 3

    X["auto_sanction_open"] = X["AU28S"] * (X["AU33S"] > 10000)
    X["mrtg_sanction_open"] = X["MT28S"] * (X["MT33S"] > 20000)
    X["secu_sanction_open"] = np.select(
        condlist=[(X["Secured Balances Sum"] - (X["AU33S"] * (X["AU33S"] > 0)) - (X["MT33S"] * (X["MT33S"] > 0))) > 0],
        choicelist=[X["Secured Balances Sum"] - (X["AU33S"] * (X["AU33S"] > 0)) - (X["MT33S"] * (X["MT33S"] > 0))],
        default=0,
    )

    X["pl_sanction_open"] = X["PL28S"] * (X["PL33S"] > 10000)
    X["cc_sanction_open"] = X["BC28S"]

    X["Total_EMI_due"] = (
        (X["auto_sanction_open"].clip(0, None) * 0.02)
        + (X["mrtg_sanction_open"].clip(0, None) * 0.008)
        + (X["secu_sanction_open"].clip(0, None) * 0.01)
        + (X["Unsecured Balances Sum"].clip(0, None) * 0.05)
    )

    X["Expected_Salary"] = X["Total_EMI_due"] / X["foir_mean"]

    X["Expected_Salary"] = np.select(
        condlist=[
            ((X["Expected_Salary"] < 22000) & (X["cc_sanction_open"] >= 10000) & (X["cc_sanction_open"] <= 66000)),
            ((X["Expected_Salary"] < 150000) & (X["cc_sanction_open"] > 450000)),
            ((X["Expected_Salary"] < 0.3 * X["cc_sanction_open"]) & (X["cc_sanction_open"] >= 60000)),
            X["Expected_Salary"] < 7500,
        ],
        choicelist=[
            23000,
            150000,
            0.35 * X["cc_sanction_open"],
            X["pred_salary"] * 0.8,
        ],
        default=X["Expected_Salary"],
    )

    X["auto_surrogate_sal"] = np.select(
        condlist=[
            X["AU28S"] <= 200000,
            X["AU28S"] <= 500000,
            X["AU28S"] <= 1000000,
            X["AU28S"] > 1000000,
        ],
        choicelist=[
            20000,
            X["AU28S"] * 0.08,
            X["AU28S"] * 0.11,
            X["AU28S"] * 0.14,
        ],
        default=[33000],
    )
    X["mrtg_surrogate_sal"] = np.select(
        condlist=[
            X["MT28S"] <= 200000,
            X["MT28S"] <= 1000000,
            X["MT28S"] <= 7500000,
            X["MT28S"] > 7500000,
        ],
        choicelist=[
            40000,
            X["MT28S"] * 0.05,
            X["MT28S"] * 0.04,
            X["MT28S"] * 0.038,
        ],
        default=[33000],
    )
    X["prsn_surrogate_sal"] = np.select(
        condlist=[
            X["PL28S"] <= 25000,
            X["PL28S"] <= 100000,
            X["PL28S"] <= 200000,
            X["PL28S"] <= 350000,
            X["PL28S"] > 350000,
        ],
        choicelist=[12000, 23000, 28000, 32000, X["PL28S"] * 0.125],
        default=[33000],
    )
    X["card_surrogate_sal"] = X["BC28S"] * 0.3
    X["minsal"] = 15000
    X["maxsal"] = 500000
    X["Current_Salary"] = X[
        [
            "auto_surrogate_sal",
            "mrtg_surrogate_sal",
            "prsn_surrogate_sal",
            "card_surrogate_sal",
            "Expected_Salary",
            "minsal",
        ]
    ].max(axis=1)
    X["Current_Salary"] = X[["Current_Salary", "maxsal"]].min(axis=1)
    X["available_income"] = (X["Current_Salary"] * 0.7) - X["Total_EMI_due"]
    data[
        [
            "foir_dlq",
            "foir_cibil",
            "foir_trd",
            "foir_mean",
            "Current_Salary",
            "Total_EMI_due",
            "available_income"
        ]
    ] = X[
        [
            "foir_dlq",
            "foir_cibil",
            "foir_trd",
            "foir_mean",
            "Current_Salary",
            "Total_EMI_due",
            "available_income"
        ]
    ]
    return data
