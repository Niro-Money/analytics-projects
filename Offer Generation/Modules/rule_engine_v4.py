import random
import warnings
import numpy as np
import pandas as pd
from __init__ import *
from Modules.incomeCalculator import availableIncomeEstimator
from Modules.riskmodel import predictRisk

warnings.filterwarnings("ignore")
random.seed(43)


@validation
@applyFilter
def initialLoanAmountCalculation(data: pd.DataFrame) -> pd.DataFrame:
    data["freeIncomeMult"] = list(
        map(
            lambda rb1, rb2: FREE_INCOME_MULT[rb1][rb2],
            data["riskband"],
            data["riskband2"],
        )
    )
    data["max_loan_possible"] = (
        (
            (data["available_income"] * data["max_tenor"] * data["freeIncomeMult"])
            // 1000
        )
    ) * 1000
    data["max_sal_loan"] = np.select(
        condlist=[
            data["Current_Salary"] < 25000,
            data["Current_Salary"] < 33000,
        ],
        choicelist=[75000, 100000],
        default=data["max_loan_possible"],
    )
    data["max_cat_loan"] = data["riskband"].map(MAX_CAT_LOAN_MAP)
    data["max_cat2_loan"] = (
        data["riskband2"].map(MAX_CAT2_LOAN_MULT) * data["max_cat_loan"]
    )
    data["offer_amount"] = data[
        [
            "max_loan_possible",
            "max_cat_loan",
            "max_sal_loan",
            "max_cat2_loan",
            "lender_amount",
        ]
    ].min(axis=1) * data["testControlTag"].map(AMOUNT_TEST_MAP)
    data["dec_reason"] = decReasonStack(data, "No Disposable Income")
    data["dec_reason"] = decReasonStack(data, f"offer_amount<{MIN_AMT_T + DEC_AMT_T}")
    return data


@validation
def decReasonStack(data: pd.DataFrame, reason: str) -> np.array:
    reasons = {
        "No Disposable Income": {
            "conditions": [
                (data["dec_reason"] == "NOT DECLINED")
                & (data["offer_amount"] < MIN_AMT_T)
            ],
            "choices": ["No Disposable Income"],
            "default": data["dec_reason"],
        },
        f"FOIR > {NEW_FOIR_T}%": {
            "conditions": [
                (data["dec_reason"] == "NOT DECLINED") & (data["New_FOIR"] > NEW_FOIR_T)
            ],
            "choices": [f"NEW FOIR > {NEW_FOIR_T}%"],
            "default": data["dec_reason"],
        },
        f"FOIR>{NEW_FOIR_T}% with New EMI": {
            "conditions": [
                (
                    (data["dec_reason"] == "NOT DECLINED")
                    & (data["max_emi"] > 0)
                    & (data["max_emi"] <= MIN_EMI_T)
                )
            ],
            "choices": [f"FOIR>{NEW_FOIR_T}% with New EMI"],
            "default": data["dec_reason"],
        },
        f"offer_amount<{MIN_AMT_T + DEC_AMT_T}": {
            "conditions": [
                (
                    (data["dec_reason"] == "NOT DECLINED")
                    & (data["offer_amount"] < MIN_AMT_T + DEC_AMT_T)
                )
            ],
            "choices": [f"offer_amount<{MIN_AMT_T + DEC_AMT_T}"],
            "default": data["dec_reason"],
        },
    }
    req = reasons[reason]
    return np.select(
        condlist=req["conditions"], choicelist=req["choices"], default=req["default"]
    )


@validation
def policyAdapter(data: pd.DataFrame) -> pd.DataFrame:
    # to deal with FOIR declined cases
    req_cols = ["max_emi", "dec_reason", "max_tenor"]
    sample = data[
        data["dec_reason"].isin(
            [f"FOIR>{NEW_FOIR_T}% with New EMI", f"FOIR > {NEW_FOIR_T}%"]
        )
    ]
    for tenor in [24, 18, 12]:
        temp = sample.copy()
        temp["max_tenor"] = tenor
        temp["amort"] = (1 + temp["roi_m"]) ** temp["max_tenor"]
        temp["max_emi"] = (
            temp["offer_amount"] * (temp["roi_m"] * temp["amort"]) / (temp["amort"] - 1)
        )
        temp["dec_reason"] = np.where(
            (temp["max_emi"] >= MIN_EMI_T) & (temp["offer_amount"] > MIN_AMT_T),
            "NOT DECLINED",
            temp["dec_reason"],
        )
        idx = temp[temp["dec_reason"] == "NOT DECLINED"].index
        sample.loc[pd.Index(idx), req_cols] = temp.loc[pd.Index(idx), req_cols]
    data.loc[pd.Index(idx), req_cols] = sample.loc[pd.Index(idx), req_cols]
    return data


@validation
@applyFilter
def initialEmiCalculation(data: pd.DataFrame) -> pd.Series:
    data["roi_m"] = data["rate_of_interest"] / 1200
    data["amort"] = (1 + data["roi_m"]) ** data["max_tenor"]
    data["max_emi"] = (
        (data["offer_amount"] * (data["roi_m"] * data["amort"]) / (data["amort"] - 1))
        / 100
    ).fillna(0).astype(int) * 100

    return data


@validation
@applyFilter
def loanAmountCalculation(data: pd.DataFrame) -> pd.Series:
    if set(["rate_of_interest", "max_tenor", "procfee", "max_emi"]) - set(data.columns):
        raise ValueError("required columns are not available")
    data["roi_m"] = data["rate_of_interest"] / 1200
    data["amort"] = (1 + data["roi_m"]) ** data["max_tenor"]
    data["l2"] = data["max_emi"] * (data["amort"] - 1) / (data["roi_m"] * data["amort"])
    data["l3"] = data["l2"] / (
        1 + (data["procfee"] / 100) + ((GST_PERC / 100) * (data["procfee"] / 100))
    )
    data["l4"] = 1000 * ((data["l3"] / 1000).fillna(0).astype(int))
    data["offer_amount"] = data["l4"]
    data["offer_amount"] = np.where(
        (data["offer_amount"] >= 49000) & (data["offer_amount"] <= 51000),
        51000,
        data["offer_amount"],
    )
    data["dec_reason"] = decReasonStack(data, "No Disposable Income")
    data["dec_reason"] = decReasonStack(data, f"offer_amount<{MIN_AMT_T + DEC_AMT_T}")
    return data


@validation
@applyFilter
def lenderAssignment(data: pd.DataFrame) -> pd.DataFrame:
    data["lending partner"] = np.select(
        condlist=[
            (data["PayU_serviceable"] == "yes"),
            (data["Muthoot_serviceable"] == "yes"),
        ],
        choicelist=["PayU", "PayU"],
        default="Liquiloans",
    )
    data["lender_amount"] = data["lending partner"].map(
        lambda x: LENDERS[x]["maxAmount"]
    )
    data["lender_tenor"] = data["lending partner"].map(lambda x: LENDERS[x]["maxTenor"])
    return data


@validation
@applyFilter
def tenorSelection(data: pd.DataFrame) -> pd.Series:
    data["max_tenor"] = data["riskband"].map(TENOR_MAP)
    data["max_tenor"] = data["max_tenor"] + data["testControlTag"].map(
        TENOR_BUREDEN_TEST_MAP
    )
    data["gmax_Ten"] = data["riskband"].map(TENOR_MAX_MAP)

    data["new_tenor"] = data[["max_tenor", "gmax_Ten", "lender_tenor"]].min(axis=1)
    return data["new_tenor"]


@validation
@applyFilter
def rateAssignment(data: pd.DataFrame) -> pd.Series:
    data["rate_temp"] = list(map(lambda x, y: RATE_MAP[x][y], data["riskband"], data["riskband2"]))
    # data["rate_of_interest"] = (
    #     data["riskband"].map(RATE_MAP)
    #     + data["riskband2"].map(RATE_MULT_MAP)
    #     + data["testControlTag"].map(RATE_TEST_MAP)
    # )
    data['rate_of_interest'] = data['rate_temp'] + \
                                 data['riskband2'].map(RATE_MULT_MAP) +\
                                 data['testControlTag'].map(RATE_TEST_MAP)
    return data["rate_of_interest"]


def testControl2(row: pd.Series) -> str:
    return "Control"

@validation
@applyFilter
def emiWithNewFoir(data: pd.DataFrame) -> pd.DataFrame:
    data["New_FOIR"] = (
        (data["max_emi"] + data["Total_EMI_due"] + 10000) * 100 / data["Current_Salary"]
    )
    data["New_FOIR"].fillna(0, inplace=True)
    data["max_emi"] = np.select(
        condlist=[data["New_FOIR"] >= NEW_FOIR_T],
        choicelist=[
            ((NEW_FOIR_T / 100) * data["Current_Salary"])
            - data["Total_EMI_due"]
            - 10000
        ],
        default=data["max_emi"],
    )
    data["dec_reason"] = decReasonStack(data, "No Disposable Income")
    data["dec_reason"] = decReasonStack(data, f"FOIR>{NEW_FOIR_T}% with New EMI")
    data = policyAdapter(data)
    return data


@validation
def rule_engine(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    data["dec_reason"] = APPROVED
    data["testControlTag"] = list(map(testControl2, data["Member Reference"]))
    data = availableIncomeEstimator(data)
    data[["riskband", "riskband2", "naps_tu", "naps_tu_thirtyplus"]] = predictRisk(
        data
    )
    data["dec_reason"] = applyBureau(data)
    data = lenderAssignment(data)
    data["max_tenor"] = tenorSelection(data)
    data = initialLoanAmountCalculation(data)
    data["rate_of_interest"] = rateAssignment(data)
    data["procfee"] = data["riskband"].map(PF_MAP)
    data = initialEmiCalculation(data)
    data = emiWithNewFoir(data)
    data = loanAmountCalculation(data)

    # params passed to lender
    data["ImputedInc"] = data["Current_Salary"].clip(20000, 400000)
    data["Current_Salary"] = np.select(
        condlist=[data["ImputedInc"] < 25000, data["ImputedInc"] <= 30000],
        choicelist=[data["ImputedInc"] + 15000, data["ImputedInc"] + 10000],
        default=data["ImputedInc"] * 1.2,
    )
    resultViewer(data)
    data.rename(columns = {'procfee':'SYSTEM_PF', 'rate_of_interest':'SYSTEM_RATEOFINT', 'offer_amount':'SYSTEM_LOAN_OFFER', 'max_emi':'SYSTEM_MAX_EMI'},inplace=True)
    return data


def applyBureau(data: pd.DataFrame) -> pd.Series:
    d = {
        "PAN Error": data[data["PAN_NO_Checked"] == "wrong pan"].index,
        "age<21": data[data["Calculated_Age"] < 21].index,
        # "age>57": data[data["Calculated_Age"] > 57].index,
        "Transgender": data[data["gender"] == "Transgender"].index,
        f"CIBIL_LT_{CIBIL_T}": data[data["CIBILTUSC3 Score Value"] < CIBIL_T].index,
        "Months on Bureau<12": data[data["AT20S"] < 12].index,
        "1+ Trade60+ in 2Y": data[(data["CV11_24M"] + data["CV12"]) > 0].index,
        "1+ Trade30+ in 6M": data[data["G310S_6M"] > 1.5].index,
        "1+ Trade 1DPD+ in 3M": data[data["G310S_3M"] > 1].index,
        "1+ Trade 60+ ever": data[data["G310S"] >= 2].index,
        "Past Write-off": data[data["CO01S180"] > 0].index,
        "10+ Inq in lst 6Mth": data[data["CV14_6M"] >= 8].index,
        "2+ Trades opened in last 3Month": data[data["AT09S_3M"] >= 2].index,
        "90+% Bank Card Utilization in last 12 month": data[
            (data["AGG911"] > 95)
            & ((data["BCPMTSTR"] == "REVOLVER") | (data["BCPMTSTR"] == "RVLRPLUS"))
        ].index,
        "90+% Bank Card Utilization": data[
            (data["AGGS911"] > 90)
            & ((data["BCPMTSTR"] == "REVOLVER") | (data["BCPMTSTR"] == "RVLRPLUS"))
        ].index,
        "Income<25K": data[data["Current_Salary"] < 26000].index,
        "1.No Disposable Income": data[
            (data["Current_Salary"] - data["Total_EMI_due"] - 13000) < 3000
        ].index,
        f"FOIR>{NEW_FOIR_T}%": data[
            ((data["Total_EMI_due"] + MIN_EMI_T) / data["Current_Salary"])
            > (NEW_FOIR_T / 100)
        ].index,
        "2.No Disposable Income": data[data["available_income"] < 3000].index,
        "1.H Risk Account": data[
            (
                (data["available_income"] < 4000)
                & (data["Current_Salary"] < 30000)
                & (data["CIBILTUSC3 Score Value"] < CIBIL_T + 40)
            )
        ].index,
        "2.H Risk Account": data[
            (
                (data["available_income"] < 4000)
                & (data["Current_Salary"] < 30000)
                & (data["TRD"] <= 3)
                & (data["CV14_6M"] >= 6)
            )
        ].index,
        "NOBC with 8+ Inq in 12M": data[
            (
                (data["Current_Salary"] > 26000)
                & (data["BCPMTSTR"] == "NOBC")
                & (data["CV14_12M"] >= 8)
            )
        ].index,
        "NOBC with 3+ Inq in 6M": data[
            (
                (data["Current_Salary"] > 26000)
                & (data["BCPMTSTR"] == "NOBC")
                & (data["CV14_12M"] < 8)
                & (data["CIBILTUSC3 Score Value"] < 732)
                & (data["CV14_6M"] > 3)
            )
        ].index,
        "NOBC with Cibil LT 750": data[
            (
                (data["Current_Salary"] > 26000)
                & (data["BCPMTSTR"] == "NOBC")
                & (data["CV14_12M"] < 8)
                & (data["CIBILTUSC3 Score Value"] > 732)
                & (data["AT20S"] <= 32)
                & (data["AT09S_12M"] > 2)
                & (data["CIBILTUSC3 Score Value"] < 751)
            )
        ].index,
        "High Credit Sum with 2+ Inq in 3M": data[
            (
                (data['Unsecured High Credit Sum']<=82000) 
                & (data['CV14_3M']>=2) 
                & (data['AT01S']>=3)
            )
        ].index,
        "naps below thresholds": data[data["riskband2"] == "JUNK"].index,
    }
    for tag, condition in d.items():
        idx = set(list(condition)).intersection(
            set(list(data[data["dec_reason"] == "NOT DECLINED"].index))
        )
        data.loc[pd.Index(idx), "dec_reason"] = tag
    return data["dec_reason"]


@applyFilter
def resultViewer(data: pd.DataFrame) -> None:
    func = lambda x: round(x * 100 / data.shape[0], 2)
    lenderFunc = lambda x: data[data["lending partner"] == x].shape[0]
    catFunc = lambda x: data[data["riskband"] == x].shape[0]
    logger.info(
        f"""
        Muthoot : {lenderFunc('Muthoot')} - {func(lenderFunc('Muthoot'))}%
        PayU : {lenderFunc('PayU')} - {func(lenderFunc('PayU'))}%
        LiquiLoans : {lenderFunc('Liquiloans')} - {func(lenderFunc('Liquiloans'))}%
        """
    )
    logger.info(
        f"""
        CAT-A : {catFunc('CAT-A')} - {func(catFunc('CAT-A'))}%
        CAT-B : {catFunc('CAT-B')} - {func(catFunc('CAT-B'))}%
        CAT-C : {catFunc('CAT-C')} - {func(catFunc('CAT-C'))}%
        CAT-D : {catFunc('CAT-D')} - {func(catFunc('CAT-D'))}%
        """
    )
    return None

