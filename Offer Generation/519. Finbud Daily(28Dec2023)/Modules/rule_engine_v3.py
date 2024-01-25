import logging
import random
import warnings
import typing
import numpy as np
import pandas as pd
from functools import lru_cache
from __init__ import *
from Modules.incomeCalculator import availableIncomeEstimator
from Modules.riskmodel import predictRisk

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO)

random.seed(43)


def score_bkt(row):
    if row <= 600:
        return "<=600"
    elif row <= 620:
        return "601-620"
    elif row <= 640:
        return "621-640"
    elif row <= 660:
        return "641-660"
    elif row <= 680:
        return "661-680"
    elif row <= 700:
        return "681-700"
    elif row <= 720:
        return "701-720"
    elif row <= 740:
        return "721-740"
    elif row <= 760:
        return "741-760"
    elif row <= 780:
        return "761-780"
    elif row <= 800:
        return "781-800"
    else:
        return "800+"


def apply_bureau(row):
    if row["PAN_NO_Checked"] == "wrong pan":
        return "PAN Error"
    elif row["Calculated_Age"] < 21:
        return "age<21"
    elif row["Calculated_Age"] > 57:
        return "age>57"
    elif row["gender"] == "Transgender":
        return "Transgender"
    elif row["CIBILTUSC3 Score Value"] < CIBIL_T:
        return f"CIBIL_LT_{CIBIL_T}"
    elif row["AT20S"] < 12:
        return "Months on Bureau<12"
    elif row["CV11_24M"] + row["CV12"] > 0:
        return "1+ Trade60+ in 2Y"
    elif row["G310S_6M"] > 1.5:
        return "1+ Trade30+ in 6M"
    elif row["G310S_3M"] > 1:
        return "1+ Trade 1DPD+ in 3M"
    elif row["G310S"] >= 2:
        return "1+ Trade 60+ ever"
    elif row["CO01S180"] > 0:
        return "Past Write-off"
    elif row["CV14_6M"] >= 8:
        return "10+ Inq in lst 6Mth"
    elif row["AT09S_3M"] >= 2:
        return "2+ Trades opened in last 3Month"
    elif (row["AGG911"] > 95) & (row["BCPMTSTR"] in ["REVOLVER", "RVLRPLUS"]):
        return "90+% Bank Card Utilization in last 12 month"
    elif (row["AGGS911"] > 90) & (row["BCPMTSTR"] in ["REVOLVER", "RVLRPLUS"]):
        return "90+% Bank Card Utilization"
    elif row["Current_Salary"] < 26000:
        return "Income<25K"
    elif row["Current_Salary"] - row["Total_EMI_due"] - 13000 < 3000:
        return "1.No Disposable Income"
    elif (row["Total_EMI_due"] + min_emi_T) / row["Current_Salary"] > NEW_FOIR_T / 100:
        return f"FOIR>{NEW_FOIR_T}%"
    elif row["available_income"] < 3000:
        return "2.No Disposable Income"
    elif (
        (row["available_income"] < 4000)
        & (row["Current_Salary"] < 30000)
        & (row["CIBILTUSC3 Score Value"] < CIBIL_T + 40)
    ):
        return "1.H Risk Account"
    elif (
        (row["available_income"] < 4000) & (row["Current_Salary"] < 30000) & (row["TRD"] <= 3) & (row["CV14_6M"] >= 6)
    ):
        return "2.H Risk Account"
    elif (row["Current_Salary"] > 26000) & (row["BCPMTSTR"] == "NOBC") & (row["CV14_12M"] >= 8):
        return "NOBC with 8+ Inq in 12M"
    elif (
        (row["Current_Salary"] > 26000)
        & (row["BCPMTSTR"] == "NOBC")
        & (row["CV14_12M"] < 8)
        & (row["CIBILTUSC3 Score Value"] < 732)
        & (row["CV14_6M"] > 3)
    ):
        return "NOBC with 3+ Inq in 6M"
    elif (
        (row["Current_Salary"] > 26000)
        & (row["BCPMTSTR"] == "NOBC")
        & (row["CV14_12M"] < 8)
        & (row["CIBILTUSC3 Score Value"] > 732)
        & (row["AT20S"] <= 32)
        & (row["AT09S_12M"] > 2)
        & (row["CIBILTUSC3 Score Value"] < 751)
    ):
        return "NOBC with Cibil LT 750"
    elif row["riskband2"] == "JUNK":
        return "naps below thresholds"
    else:
        return "NOT DECLINED"


def testControl2(row):
    return "Control"


def approvedRecordsFilter(data: pd.DataFrame) -> typing.Union[pd.DataFrame, list]:
    validation(data)
    approved = data[data["dec_reason"] == "NOT DECLINED"]
    idx = approved.index
    return approved, idx


def initialLoanAmountCalculation(data: pd.DataFrame) -> pd.Series:
    validation(data)
    approved, idx = approvedRecordsFilter(data)
    logging.info(f"Calculating initial loan Amount for {approved.shape[0]} records")
    approved["max_loan_possible"] = (
        ((approved["available_income"] * approved["max_tenor"]) / 1000).astype(int)
    ) * 1000
    approved["max_sal_loan"] = np.select(
        condlist=[
            approved["Current_Salary"] < 25000,
            approved["Current_Salary"] < 33000,
        ],
        choicelist=[75000, 100000],
        default=approved["max_loan_possible"],
    )
    approved["max_cat_loan"] = list(
        map(lambda rb1, rb2: maxcat2loan_map[rb1][rb2], approved["riskband"], approved["riskband2"])
    )
    approved["SYSTEM_LOAN_OFFER"] = approved[["max_loan_possible", "max_cat_loan", "max_sal_loan"]].min(
        axis=1
    ) * approved["testControlTag"].map(amountTestMap)
    data.loc[idx, "SYSTEM_LOAN_OFFER"] = approved["SYSTEM_LOAN_OFFER"]
    return data["SYSTEM_LOAN_OFFER"]


def decReasonStack(data: pd.DataFrame, reason: str) -> np.array:
    validation(data)
    for col in ['SYSTEM_LOAN_OFFER','SYSTEM_MAX_EMI']:
        if col not in data.columns:
            logging.info(f"creating a temp {col} column")
            data[col] = 0
    reasons = {
        "No Disposable Income": {
            "conditions": [
                data["SYSTEM_LOAN_OFFER"] <= 0,
                ((data["dec_reason"] == "NOT DECLINED") & (data["SYSTEM_LOAN_OFFER"] < min_amt_T)),
            ],
            "choices": [data["dec_reason"], "No Disposable Income"],
            "default": data["dec_reason"],
        },
        f"FOIR>{NEW_FOIR_T}% with New EMI": {
            "conditions": [
                (
                    (data["dec_reason"] == "NOT DECLINED")
                    & (data["SYSTEM_MAX_EMI"] > 0)
                    & (data["SYSTEM_MAX_EMI"] <= min_emi_T)
                )
            ],
            "choices": [f"FOIR>{NEW_FOIR_T}% with New EMI"],
            "default": data["dec_reason"],
        },
        "3.No Disposable Income": {
            "conditions": [((data["dec_reason"] == "NOT DECLINED") & (data["SYSTEM_LOAN_OFFER"] < min_amt_T))],
            "choices": ["3.No Disposable Income"],
            "default": data["dec_reason"],
        },
        f"SYSTEM_LOAN_OFFER<{min_amt_T + DEC_AMT_T}": {
            "conditions": [
                ((data["dec_reason"] == "NOT DECLINED") & (data["SYSTEM_LOAN_OFFER"] < min_amt_T + DEC_AMT_T))
            ],
            "choices": [f"SYSTEM_LOAN_OFFER<{min_amt_T + DEC_AMT_T}"],
            "default": data["dec_reason"],
        },
        # "Pincode_not_serviceable": {
        #     "conditions": [
        #         (
        #             (data["PayU_serviceable"] == "no")
        #             & (data["Muthoot_serviceable"] == "no")
        #             & (data["LL_serviceable"] == "no")
        #         )
        #     ],
        #     "choices": ["Pincode_not_serviceable"],
        #     "default": data["dec_reason"],
        # },
    }
    req = reasons[reason]
    return np.select(condlist=req["conditions"], choicelist=req["choices"], default=req["default"])


def initialEmiCalculation(data: pd.DataFrame) -> pd.Series:
    validation(data)
    approved, idx = approvedRecordsFilter(data)
    logging.info(f"calculating initial EMI for {approved.shape[0]} records")
    approved["roi_m"] = approved["SYSTEM_RATEOFINT"] / 1200
    approved["amort"] = (1 + approved["roi_m"]) ** approved["max_tenor"]
    approved["SYSTEM_MAX_EMI"] = (
        (approved["SYSTEM_LOAN_OFFER"] * (approved["roi_m"] * approved["amort"]) / (approved["amort"] - 1)) / 100
    ).fillna(0).astype(int) * 100
    data.loc[idx, "SYSTEM_MAX_EMI"] = approved["SYSTEM_MAX_EMI"]
    return data["SYSTEM_MAX_EMI"]


def finalEmiCalculation(data: pd.DataFrame) -> pd.Series:
    validation(data)
    approved, idx = approvedRecordsFilter(data)
    logging.info(f"calculating final EMI for {approved.shape[0]} records")
    if set(["max_tenor", "SYSTEM_LOAN_OFFER"]) - set(approved.columns):
        raise ValueError("required columns are not available")

    approved["SYSTEM_MAX_EMI"] = np.select(
        condlist=[approved["lending partner"] == "Liquiloans"],
        choicelist=[(((approved["SYSTEM_LOAN_OFFER"] / approved["max_tenor"]) / 100).fillna(0).astype(int)) * 100],
        default=approved["SYSTEM_MAX_EMI"],
    )

    data.loc[idx, "SYSTEM_MAX_EMI"] = approved["SYSTEM_MAX_EMI"]
    return data["SYSTEM_MAX_EMI"]


def loanAmountCalculation(data: pd.DataFrame) -> pd.Series:
    validation(data)
    approved, idx = approvedRecordsFilter(data)
    logging.info(f"calculating loan amount for {approved.shape[0]} records")
    if set(["SYSTEM_RATEOFINT", "max_tenor", "SYSTEM_PF", "SYSTEM_MAX_EMI"]) - set(approved.columns):
        raise ValueError("required columns are not available")
    approved["roi_m"] = approved["SYSTEM_RATEOFINT"] / 1200
    approved["amort"] = (1 + approved["roi_m"]) ** approved["max_tenor"]
    approved["l2"] = approved["SYSTEM_MAX_EMI"] * (approved["amort"] - 1) / (approved["roi_m"] * approved["amort"])
    approved["l3"] = approved["l2"] / (
        1 + (approved["SYSTEM_PF"] / 100) + ((GST_perc / 100) * (approved["SYSTEM_PF"] / 100))
    )
    approved["l4"] = 1000 * ((approved["l3"] / 1000).fillna(0).astype(int))
    n = approved[approved["l4"] < (min_amt_T + DEC_AMT_T)].shape[0]
    logging.info(f"records having loan amount <{min_amt_T+DEC_AMT_T} {n}")
    data.loc[idx, "SYSTEM_LOAN_OFFER"] = approved["l4"]
    return data["SYSTEM_LOAN_OFFER"]


def lenderAssignment(data: pd.DataFrame) -> pd.DataFrame:
    validation(data)
    approved, idx = approvedRecordsFilter(data)
    logging.info(f"initializing lender assignment for {approved.shape[0]} records")
    approved["lending partner"] = np.select(
        condlist=[
            (approved["PayU_serviceable"] == "yes")
            & (approved["SYSTEM_LOAN_OFFER"] <= lenders["payU"]["maxAmount"])
            & (approved["max_tenor"] <= lenders["payU"]["maxTenor"]),
            (approved["Muthoot_serviceable"] == "yes")
            & (approved["SYSTEM_LOAN_OFFER"] <= lenders["muthoot"]["maxAmount"])
            & (approved["max_tenor"] <= lenders["muthoot"]["maxTenor"])
            & (
                ((approved["riskband"] == "CAT-C") & (approved["riskband2"].isin(["L", "M"])))
                | (approved["riskband"] == "CAT-D")
            ),
        ],
        choicelist=["PayU", "Muthoot"],
        default="Liquiloans",
    )

    # balancing partner proportions
    muthoot_cases = approved[approved["lending partner"] == "Muthoot"]
    if muthoot_cases.shape[0] / approved.shape[0] < muthoot_threshold:
        logging.info(f"muthoot records below {muthoot_threshold*100}% initializing lender switch...")
        approved["lender_switch"] = np.select(
            condlist=[
                (approved["lending partner"].isin(["Liquiloans", "PayU"]))
                & (approved["Muthoot_serviceable"] == "yes")
                & (approved["riskband"].isin(muthoot_switchable_cats))
            ],
            choicelist=[1],
            default=0,
        )
        sample_size = int(np.ceil(muthoot_threshold * approved.shape[0] - muthoot_cases.shape[0]))
        if sample_size < approved["lender_switch"].sum():
            idx = approved[approved["lender_switch"] == 1].sample(sample_size, random_state=42).index
            logging.info(f"switching lender for {len(idx)} records")
            approved.loc[idx, "lending partner"] = "Muthoot"
        else:
            logging.info("no records to switch lender")

    # adjusting loan parameters
    approved["SYSTEM_LOAN_OFFER"] = np.select(
        condlist=[(approved["lending partner"] == "Liquiloans") & (approved["SYSTEM_LOAN_OFFER"] > 240000)],
        choicelist=[240000],
        default=approved["SYSTEM_LOAN_OFFER"],
    )
    approved["max_tenor"] = np.select(
        condlist=[(approved["lending partner"] == "Liquiloans") & (approved["max_tenor"] > 24)],
        choicelist=[24],
        default=approved["max_tenor"],
    )

    data.loc[idx, ["SYSTEM_LOAN_OFFER", "max_tenor", "lending partner"]] = approved[
        ["SYSTEM_LOAN_OFFER", "max_tenor", "lending partner"]
    ]
    return data[["SYSTEM_LOAN_OFFER", "max_tenor", "lending partner"]]


def tenorSelection(data: pd.DataFrame) -> pd.Series:
    approved, idx = approvedRecordsFilter(data)
    approved["max_tenor"] = approved["max_tenor"] + approved["testControlTag"].map(tenorBurdenTestMap)
    approved["gmax_Ten"] = approved["riskband"].map(tenorMax_map)
    approved["new_tenor"] = approved[["max_tenor", "gmax_Ten"]].min(axis=1)
    data.loc[idx, "max_tenor"] = approved["new_tenor"]
    return data["max_tenor"]


def rule_engine(data: pd.DataFrame) -> pd.DataFrame:
    validation(data)
    logging.info(f"records in rule engine {data.shape[0]}")
    dfnaps = data.copy()
    dfnaps["testControlTag"] = list(map(testControl2, dfnaps["Member Reference"]))

    # available income calcuation
    dfnaps = availableIncomeEstimator(dfnaps)
    dfnaps["available_income"] = (dfnaps["Current_Salary"] * 0.7) - dfnaps["Total_EMI_due"]

    # risk prediction
    dfnaps[["riskband", "riskband2", "naps_tu", "naps_tu_thirtyplus"]] = predictRisk(dfnaps)
    dfnaps["naps_bkt"] = list(map(score_bkt, dfnaps["naps_tu"]))
    dfnaps["cibilv3_bkt"] = list(map(score_bkt, dfnaps["CIBILTUSC3 Score Value"]))

    # preapproved niroBRE
    dfnaps["dec_reason"] = dfnaps.apply(apply_bureau, axis=1)

    # initial loan parameters
    dfnaps["max_tenor"] = list(map(lambda x, y: tenor_map[x][y], dfnaps["riskband"], dfnaps["riskband2"]))
    dfnaps["SYSTEM_PF"] = dfnaps["riskband"].map(pf_map)
    dfnaps["SYSTEM_RATEOFINT"] = list(map(lambda x, y: rate_map[x][y], dfnaps["riskband"], dfnaps["riskband2"]))
    dfnaps["SYSTEM_LOAN_OFFER"] = initialLoanAmountCalculation(dfnaps)
    dfnaps["dec_reason"] = decReasonStack(dfnaps, "No Disposable Income")

    # loan amounts calculation
    dfnaps["max_tenor"] = tenorSelection(dfnaps)
    dfnaps["SYSTEM_RATEOFINT"] = dfnaps["SYSTEM_RATEOFINT"] + dfnaps["testControlTag"].map(rateTestMap)
    dfnaps["SYSTEM_MAX_EMI"] = initialEmiCalculation(dfnaps)
    dfnaps["New_FOIR"] = (dfnaps["SYSTEM_MAX_EMI"] + dfnaps["Total_EMI_due"] + 10000) * 100 / dfnaps["Current_Salary"]
    dfnaps["New_FOIR"] = dfnaps["New_FOIR"].fillna(0)
    dfnaps["SYSTEM_MAX_EMI"] = np.select(
        condlist=[dfnaps["New_FOIR"] >= NEW_FOIR_T],
        choicelist=[((NEW_FOIR_T / 100) * dfnaps["Current_Salary"]) - dfnaps["Total_EMI_due"] - 10000],
        default=dfnaps["SYSTEM_MAX_EMI"],
    )
    dfnaps["dec_reason"] = decReasonStack(dfnaps, f"FOIR>{NEW_FOIR_T}% with New EMI")
    dfnaps["SYSTEM_LOAN_OFFER"] = loanAmountCalculation(dfnaps)
    dfnaps["dec_reason"] = decReasonStack(dfnaps, "3.No Disposable Income")
    dfnaps["dec_reason"] = decReasonStack(dfnaps, f"SYSTEM_LOAN_OFFER<{min_amt_T + DEC_AMT_T}")
    dfnaps["ImputedInc"] = dfnaps["Current_Salary"].clip(20000, 400000)
    dfnaps["Current_Salary"] = np.select(
        condlist=[dfnaps["ImputedInc"] < 25000, dfnaps["ImputedInc"] <= 30000],
        choicelist=[dfnaps["ImputedInc"] + 15000, dfnaps["ImputedInc"] + 10000],
        default=dfnaps["ImputedInc"] * 1.2,
    )

    # lender assignment and modifications according to lender
    dfnaps["lending partner"] = "unclaimed"
    dfnaps[["SYSTEM_LOAN_OFFER", "max_tenor", "lending partner"]] = lenderAssignment(dfnaps)
    #dfnaps["dec_reason"] = decReasonStack(dfnaps, "Pincode_not_serviceable")
    dfnaps["SYSTEM_MAX_EMI"] = finalEmiCalculation(dfnaps)
    dfnaps["SYSTEM_LOAN_OFFER"] = loanAmountCalculation(dfnaps)
    dfnaps["dec_reason"] = decReasonStack(dfnaps, f"SYSTEM_LOAN_OFFER<{min_amt_T + DEC_AMT_T}")
    logging.info(f"Approved and Serviceable cases {dfnaps[dfnaps['dec_reason']=='NOT DECLINED'].shape[0]}")
    resultViewer(dfnaps)
    return dfnaps


def resultViewer(dfnaps):
    validation(dfnaps)
    approved, _ = approvedRecordsFilter(dfnaps)
    muthoot = approved[approved["lending partner"] == "Muthoot"].shape[0]
    payu = approved[approved["lending partner"] == "PayU"].shape[0]
    ll = approved[approved["lending partner"] == "Liquiloans"].shape[0]
    logging.info(
        f"""
        Muthoot : {muthoot} - {round(muthoot*100/approved.shape[0],2)}%
        PayU : {payu} - {round(payu*100/approved.shape[0],2)}%
        LiquiLoans : {ll} - {round(ll*100/approved.shape[0],2)}%
        """
    )
    cat_a = approved[approved["riskband"] == "CAT-A"].shape[0]
    cat_b = approved[approved["riskband"] == "CAT-B"].shape[0]
    cat_c = approved[approved["riskband"] == "CAT-C"].shape[0]
    cat_d = approved[approved["riskband"] == "CAT-D"].shape[0]
    logging.info(
        f"""
        CAT-A : {cat_a} - {round(cat_a*100/approved.shape[0],2)}%
        CAT-B : {cat_b} - {round(cat_b*100/approved.shape[0],2)}%
        CAT-C : {cat_c} - {round(cat_c*100/approved.shape[0],2)}%
        CAT-D : {cat_d} - {round(cat_d*100/approved.shape[0],2)}%
        """
    )


def validation(data: pd.DataFrame) -> None:
    if not isinstance(data, pd.DataFrame):
        raise TypeError("expected Dataframe got something else")
