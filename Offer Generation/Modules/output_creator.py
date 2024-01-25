from datetime import date

import numpy as np
import pandas as pd
from Modules.dedupe_check import dedupe_checker
from __init__ import *

import warnings

warnings.filterwarnings("ignore")

def score_bkt(row, var):
    if row[var] <= 600:
        return '<=600'
    elif row[var] <= 620:
        return '601-620'
    elif row[var] <= 640:
        return '621-640'
    elif row[var] <= 660:
        return '641-660'
    elif row[var] <= 680:
        return '661-680'
    elif row[var] <= 700:
        return '681-700'
    elif row[var] <= 720:
        return '701-720'
    elif row[var] <= 740:
        return '721-740'
    elif row[var] <= 760:
        return '741-760'
    elif row[var] <= 780:
        return '761-780'
    elif row[var] <= 800:
        return '781-800'
    else:
        return '800+'


def pivotCreator(dfnaps):
    # pivot Table file creation
    df_pivot = dfnaps[dfnaps["dec_reason"] == "NOT DECLINED"].copy()
    req_stats = {
        "Member Reference": ["count"],
        "BC02S": ["mean"],
        "BC28S": ["mean"],
        "New_FOIR": ["mean"],
        "CV14_1M": ["max", "sum", "mean"],
        "CV14_3M": ["max", "sum", "mean"],
        "TRD": ["mean"],
        "CIBILTUSC3 Score Value": ["min", "mean"],
        "PROPENPLSC Score Value": ["min", "mean"],
        "ImputedInc": ["mean"],
        "SYSTEM_PF": ["mean"],
        "SYSTEM_RATEOFINT": ["mean"],
        "SYSTEM_LOAN_OFFER": ["min", "mean", "max"],
        "SYSTEM_MAX_EMI": ["mean"],
        "max_tenor": ["mean"],
        "naps_tu": ["min", "mean", "max"],
        # "naps_tu_thirtyplus": ["min", "mean", "max"],
    }
    req_cols = list(req_stats.keys()) + [
        "riskband",
        "riskband2",
        "lending partner",
    ]
    df_pivot = df_pivot[req_cols]

    pivot = None
    for lender in ["Muthoot", "PayU", "Liquiloans"]:
        Pivot = (
            df_pivot[df_pivot["lending partner"] == lender]
            .groupby(["riskband", "riskband2"])
            .agg(req_stats)
        )
        Pivot['lender']=lender
        pivot = pd.concat([pivot, Pivot])

    pivot = pd.concat(
        [pivot, df_pivot.groupby(["riskband", "riskband2"]).agg(req_stats)]
    )
    pivot.to_excel(SAVE_PATH / "pivot.xlsx")


# def pivot_naps_30_plusCreator(temp):
#     dfnaps = temp[temp['dec_reason']=='NOT DECLINED']
#     dfnaps['napsthirtyplus_bkt']= dfnaps.apply(score_bkt,var='naps_tu_thirtyplus',axis=1)
#     df_ann = pd.pivot_table(
#         dfnaps,
#         values="Member Reference",
#         index=["napsthirtyplus_bkt"],
#         columns=["max_tenor"],
#         aggfunc=["count"],
#         fill_value=0,
#         margins=True,
#         margins_name="Total",
#     ).rename(columns={'count':'phone_numbers(count)'})
#     df_ann1 = pd.pivot_table(
#         dfnaps,
#         values="SYSTEM_LOAN_OFFER",
#         index=["napsthirtyplus_bkt"],
#         columns=["max_tenor"],
#         aggfunc=["mean"],
#         fill_value=0,
#     ).rename(columns={'mean':'system_loan_offer(mean)'})
#     df_ann2 = pd.pivot_table(
#         dfnaps,
#         values="SYSTEM_RATEOFINT",
#         index=["napsthirtyplus_bkt"],
#         columns=["max_tenor"],
#         aggfunc=["mean"],
#         fill_value=0,
#     ).rename(columns={'mean':'rate_of_int(mean)'})
#     df = pd.concat([df_ann,df_ann1,df_ann2],axis=1,ignore_index=False)
#     df.to_excel(SAVE_PATH / "pivot_naps_thirty_plus.xlsx")


def declineReason(dfnaps):
    # pivot for Decline reason analysis
    df_analysis = dfnaps.groupby(["dec_reason"]).agg({"Member Reference": ["count"]})
    df_analysis.reset_index(inplace=True)
    df_analysis["percentage"] = (
        df_analysis["Member Reference"] * 100 / TOTAL_COUNT
    ).round(2)
    df_analysis["percentage"] = (df_analysis["percentage"].astype(str)) + "%"
    df_analysis.to_csv(SAVE_PATH / "Dec_reason_analysis.csv", index=False)


def Output_Creator(dfnaps: pd.DataFrame):
    dfnaps.rename(
        columns={
            "pred_propensity":"PROPENPLSC Score Value",
            "decline_post":"dec_reason",
            "member reference":"Member Reference",
            "bc02s":"BC02S",
            "bc28s":"BC28S",
            "foir":"New_FOIR",
            "cv14_1m":"CV14_1M",
            "cv14_3m":"CV14_3M",
            "trd":"TRD",
            "stated_income":"ImputedInc",
            "cibiltusc3_score_value":"CIBILTUSC3 Score Value",
            "naps":"naps_tu",
            "finalTenor":"max_tenor",
            "finalEMI":"SYSTEM_MAX_EMI",
            "maxLoanAmount":"SYSTEM_LOAN_OFFER",
            "finalROI":"SYSTEM_RATEOFINT",
            "finalPF":"SYSTEM_PF",
            "final_name":"Final_name",
            "city":"City",
            "payu_serviceable":"PayU_serviceable",
            "pan_no_checked":"PAN_NO_Checked",
            "ll_serviceable":"Niro_LL_serviceable",
            "subrisk":"riskband2",
        },
        inplace=True,
    )
    dfnaps['testControlTag']="control"
    dfnaps["data_source"] = DATA_SOURCE
    dfnaps["offer_source"] = OFFER_SOURCE
    dfnaps["GST_Perc"] = GST_PERC
    logger.info(
        f"performing dedupe check on {dfnaps[dfnaps['dec_reason'] == 'NOT DECLINED'].shape[0]} records"
    )
    dfnaps["randNumCol"] = np.random.randint(1, 9999, dfnaps.shape[0])
    dfnaps["Data_Tag"] = DATA_TAG + DD
    dfnaps["Platform_Partner_Name"] = PLATFORM_PARTNER
    

    dfnaps["Member Reference"] = (
        dfnaps["Member Reference"].astype(float).astype("int64").astype(str).str[-10:]
    )
    dfnaps['RULE_ENGINE_VERSION'] = RULE_ENGINE_VERSION
    dfnaps['NAPS_VERSION'] = NAPS_VERSION
    dfnaps.to_csv(SAVE_PATH / f"{OFFER_SOURCE}.csv", index=False)

    pivotCreator(dfnaps)
    # pivot_naps_30_plusCreator(dfnaps)
    declineReason(dfnaps)
    # ---SnM file creation
    dfnaps.rename(
        columns={
            "Member Reference": "Mobile_No",
            "Final_name": "Name",
            "gender": "Gender",
            "PAN_NO_Checked": "PAN_NO",
            "max_tenor": "Max_Tenor",
            "SYSTEM_MAX_EMI": "Max_EMI",
            "SYSTEM_RATEOFINT": "Interest_Rate_Perc",
            "SYSTEM_PF": "Min_Proc_Fee_Perc",
            "CIBILTUSC3 Score Value": "Bureau_score",
            "riskband": "Category",
            "email_final": "Email_ID",
            "addr1": "Bureau_Address_1",
            "addr2": "Bureau_Address_2",
            "addr3": "Bureau_Address_3",
            "pincode_approved": "PIN",
            "statename": "State",
            "naps_tu": "Naps_Score",
            "tele1":"Tele1",
            "tele2":"Tele2",
            "tele3":"Tele3",
            "tele4":"Tele4",
            "tele5":"Tele5",
            "tele6":"Tele6",
            "salary":'Current_Salary',
        },
        inplace=True,
    )
    # dfnaps['Mobile_No'] = dfnaps['Member Reference']
    dfnaps["Mobile_No"] = (
        dfnaps["Mobile_No"].astype(float).astype("int64").astype(str).str[-10:]
    )

    dfnaps["Address"] = dfnaps["Bureau_Address_1"].fillna(
        dfnaps["Bureau_Address_2"].fillna(
            dfnaps["Bureau_Address_3"].fillna(
                dfnaps["addr4"].fillna(
                    dfnaps["addr5"].fillna(dfnaps["addr6"].fillna(dfnaps["City"]))
                )
            )
        )
    )
    dfnaps["Email_List"] = (
        dfnaps["email1"].astype(str)
        + ","
        + dfnaps["email2"].astype(str)
        + ","
        + dfnaps["email3"].astype(str)
        + ","
        + dfnaps["email4"].astype(str)
        + ","
        + dfnaps["email5"].astype(str)
        + ","
        + dfnaps["email6"].astype(str)
    )
    dfnaps["ID_List"] = (
        dfnaps["id1"].astype(str)
        + ","
        + dfnaps["id2"].astype(str)
        + ","
        + dfnaps["id3"].astype(str)
        + ","
        + dfnaps["id4"].astype(str)
    )
    dfnaps["Phone_List"] = (
        dfnaps["Tele1"].astype(str)
        + ","
        + dfnaps["Tele2"].astype(str)
        + ","
        + dfnaps["Tele3"].astype(str)
        + ","
        + dfnaps["Tele4"].astype(str)
        + ","
        + dfnaps["Tele5"].astype(str)
        + ","
        + dfnaps["Tele6"].astype(str)
    )

    dfnaps["Max_Proc_Fee_Perc"] = dfnaps["Min_Proc_Fee_Perc"]
    dfnaps["Min_Disbursal_Amount"] = 50000
    dfnaps["Platform_Partner_Name"] = PLATFORM_PARTNER
    dfnaps["Disbursal_Partner_Name"] = dfnaps["lending partner"]
    dfnaps["Min_Disbursal_Amount"] = np.select(condlist = [
        ((dfnaps['Disbursal_Partner_Name']=='PayU')|(dfnaps['Category'].isin(['CAT-A','CAT-B']))),
        (dfnaps['Disbursal_Partner_Name']=='Liquiloans')
    ],choicelist=[50000,20000],default=dfnaps['Min_Disbursal_Amount'])


    dfnaps["PayU_serviceable"] = np.select(
        condlist=[(dfnaps["PayU_serviceable"] == "yes")],
        choicelist=["PayU"],
        default="Non_PayU",
    )
    # dfnaps["Muthoot_serviceable_k"] = np.select(
    #     condlist=[(dfnaps["Muthoot_serviceable"] == "yes")],
    #     choicelist=["Muthoot"],
    #     default="Non_Muthoot",
    # )

    dfnaps["Niro_LL_serviceable"] = np.select(
        condlist=[(dfnaps["PIN"] != 0)],
        choicelist=["Niro_LL_serviceable"],
        default="Non_Serviceable",
    )
    cols = ["PayU_serviceable", "Niro_LL_serviceable"]
    dfnaps["pincode_lender"] = dfnaps[cols].apply(
        lambda row: "_".join(row.values.astype(str)), axis=1
    )
    dfnaps["Data_Partner"] = DATA_PARTNER
    dfnaps["Imputed_Income(Monthly)"] = dfnaps["Current_Salary"]
    dfnaps["User_ID"] = "For Later"
    dfnaps["opportunity_id"] = "For Later"
    dfnaps["experiment_id"] = RUNDATE + "_" + dfnaps["testControlTag"].astype(str)
    dfnaps["Max_EMI"] = 50 * ((dfnaps["Max_EMI"] / 50).fillna(0).astype(int))
    dfnaps["Partner_User_ID"] = ""
    # dfnaps = dfnaps[(dfnaps['Category']!='CAT-D')&(dfnaps['riskband2']!='Hig')]
    logger.info(len(dfnaps[(dfnaps['Category']=='CAT-D')&(dfnaps['riskband2']=='Hig')]))
    dfnaps['cibilv3_bkt']= dfnaps.apply(score_bkt,var='Bureau_score',axis=1)
    dfnaps["language"] ="english"
    colsForTech = [
        "Mobile_No",
        "Name",
        "Gender",
        "Address",
        "City",
        "State",
        "Email_List",
        "PAN_NO",
        "Max_Tenor",
        "Max_EMI",
        "Interest_Rate_Perc",
        "Min_Proc_Fee_Perc",
        "Max_Proc_Fee_Perc",
        "GST_Perc",
        "Min_Disbursal_Amount",
        "Platform_Partner_Name",
        "Disbursal_Partner_Name",
        "Imputed_Income(Monthly)",
        "SYSTEM_LOAN_OFFER",
        "Bureau_score",
        "cibilv3_bkt",
        "Category",
        "Email_ID",
        "ID_List",
        "Phone_List",
        "Bureau_Address_1",
        "Bureau_Address_2",
        "Bureau_Address_3",
        "PIN",
        "User_ID",
        "opportunity_id",
        "data_source",
        "offer_source",
        "experiment_id",
        "propensity",
        "PROPENPLSC Score Value",
        "Data_Partner",
        "pincode_lender",
        "language",
        "Partner_User_ID",
        "Data_Tag",
        "Naps_Score",
        # "final_cohort"
    ]
    # pin check removal pin filling
    dfnaps["PIN"] = np.select(
        condlist=[
            (dfnaps["PIN"] == 0),
            (dfnaps["PIN"] == 999999),
            (dfnaps["PIN"] == 999998),
            (dfnaps["PIN"].isna() == True),
        ],
        choicelist=[110001, 110001, 110001, 110001],
        default=dfnaps["PIN"],
    )
    dfnaps["City"] = np.select(
        condlist=[(dfnaps["PIN"] == 110001)],
        choicelist=["New Delhi"],
        default=dfnaps["City"],
    )
    dfnaps["State"] = np.select(
        condlist=[(dfnaps["PIN"] == 110001)],
        choicelist=["Delhi"],
        default=dfnaps["State"],
    )

    dfnaps = dfnaps[dfnaps["dec_reason"] == "NOT DECLINED"]
    dfnaps["City"].fillna("New Delhi", inplace=True)
    dfnaps["State"].fillna("Delhi", inplace=True)
    df2tech = dfnaps[colsForTech]
    df2tech.to_csv(SAVE_PATH / "before_dedupe.csv")

    # Null PAN in total data
    logger.info(f"The total null pan in data: {len(dfnaps[dfnaps['PAN_NO']=='nan'])}")
    logger.info(
        f"The total null pan % in data wrt total data: {(round(len(dfnaps[dfnaps['PAN_NO']=='nan'])*100/TOTAL_COUNT,2))}"
    )

    # ---Already offered dedupe check
    df2tech["Mobile_No"] = (
        df2tech["Mobile_No"].astype(float).astype("int64").astype(str).str[-10:]
    )
    df2tech_passed2 = dedupe_checker(df2tech)
    logger.info(f"records after dedupe {len(df2tech_passed2)}")
    logger.info(f"records failed in dedupe {len(dfnaps) - len(df2tech_passed2)}")
    df2tech_dedupe_failed = dfnaps[~dfnaps["Mobile_No"].isin(df2tech_passed2["Mobile_No"])]
    df2tech_dedupe_failed.to_csv(SAVE_PATH / "df2tech_dedupe_failed.csv", index=False)

    ##############################################################################################################################################
    logger.info(f"records qualified for uploading {len(df2tech_passed2)}")
    df2tech_passed2["Mobile_No"] = (
        df2tech_passed2["Mobile_No"].astype(float).astype("int64").astype(str).str[-10:]
    )
    # updated Data_Tag with number of offers generated
    df2tech_passed2["Data_Tag"] = DATA_TAG + str(len(df2tech_passed2)) + DD
    df2tech_passed2['hashed_phone']=''
    df2tech_passed2.to_csv(SAVE_PATH / f"Offer_{OFFER_SOURCE}.csv", index=False)
    logger.info(f"""
                 Lenders
                 Muthoot {len(df2tech_passed2[df2tech_passed2["Disbursal_Partner_Name"] == "Muthoot"])}
                 PayU {len(df2tech_passed2[df2tech_passed2["Disbursal_Partner_Name"] == "PayU"])},
                 Liquiloans {len(df2tech_passed2[df2tech_passed2["Disbursal_Partner_Name"] == "Liquiloans"])}
                 """),
    
    
    # ## Dedupe Update
    df2tech_dedupe_failed = df2tech_dedupe_failed[
        [
            "Mobile_No",
            "Name",
            "Gender",
            "Address",
            "Email_List",
            "PAN_NO",
            "Max_Tenor",
            "Max_EMI",
            "Interest_Rate_Perc",
            "Min_Proc_Fee_Perc",
            "Max_Proc_Fee_Perc",
            "GST_Perc",
            "Min_Disbursal_Amount",
            "Disbursal_Partner_Name",
            "Imputed_Income(Monthly)",
            "Bureau_score",
            "Category",
            "Email_ID",
            "ID_List",
            "Phone_List",
            "Bureau_Address_1",
            "Bureau_Address_2",
            "Bureau_Address_3",
            "User_ID",
            "opportunity_id",
            "PIN",
            "City",
            "State",
            "experiment_id",
            "propensity",
            "PROPENPLSC Score Value",
        ]
    ]
    df2tech_dedupe_failed.to_csv(SAVE_PATH / "To_update_dedupe_failed.csv", index=False)
    # offer split to tech
    df2tech_passed2 = df2tech_passed2[
        [
            "Mobile_No",
            "Name",
            "Gender",
            "Address",
            "Email_List",
            "PAN_NO",
            "Max_Tenor",
            "Max_EMI",
            "Interest_Rate_Perc",
            "Min_Proc_Fee_Perc",
            "Max_Proc_Fee_Perc",
            "GST_Perc",
            "Min_Disbursal_Amount",
            "Platform_Partner_Name",
            "Disbursal_Partner_Name",
            "Imputed_Income(Monthly)",
            "Bureau_score",
            "Category",
            "Email_ID",
            "ID_List",
            "Phone_List",
            "Bureau_Address_1",
            "Bureau_Address_2",
            "Bureau_Address_3",
            "User_ID",
            "opportunity_id",
            "PIN",
            "City",
            "State",
            "experiment_id",
            "propensity",
            "PROPENPLSC Score Value",
            "Partner_User_ID",
            "Data_Tag",
            "Naps_Score",
            "hashed_phone"
        ]
    ]
    df2tech_passed2 = df2tech_passed2.replace(r"\\", "", regex=True)
    df2tech_passed2["Address"] = df2tech_passed2["Address"].fillna(
        df2tech_passed2["City"]
    )
    df2tech_passed2["PAN_NO"] = df2tech_passed2["PAN_NO"].fillna('nan'
    )
    df2tech_passed2['offer_type'] = ''
    df2tech_passed2['cohort'] = ''
    df2tech_passed2.rename(columns={'PROPENPLSC Score Value':'propensity_score','Imputed_Income(Monthly)':'imputed_income_monthly','Email_ID':'email'},inplace=True)
    df2tech_passed2['Mobile_No'] = df2tech_passed2['Mobile_No'].astype(float).astype('int64').astype(str)
    df2tech_passed2['PIN'] = df2tech_passed2['PIN'].astype(float).astype('int64').astype(str)

    df2tech_passed2.to_csv(
        SAVE_PATH / f"{OFFER_SOURCE}_consolidated(marketing).csv", index=False
    )
    # fragments_df = np.array_split(df2tech_passed2, 8)
    # for i in range(len(fragments_df)):
    #     logger.info(f"length of offer-{DATA_SOURCE}_part{str(i)} - {len(fragments_df[i])}"),
    #     fragments_df[i].to_csv(
    #         SAVE_PATH / f"offer-T-{DATA_SOURCE}_part{i}.csv",
    #         index=False,
    #     )

    # Null PAN in offers
    logger.info(f"The total null pan in offers: {len(df2tech_passed2[df2tech_passed2['PAN_NO']=='nan'])}")
    logger.info(f"The total null pan % in offers wrt total offers: {(round(len(df2tech_passed2[df2tech_passed2['PAN_NO']=='nan'])*100/len(df2tech_passed2),2))}")

    # --creating category-propensity pivot
    df_ann = pd.pivot_table(
        df2tech_passed2,
        values="Mobile_No",
        index=["Category"],
        columns=["propensity"],
        aggfunc=["count"],
        fill_value=0,
        margins=True,
        margins_name="Total",
    )
    df_ann["percentage"] = (
        (df_ann[("count", "Total")]) * 100 / TOTAL_COUNT
    ).round(2)
    df_ann["percentage"] = (df_ann["percentage"].astype(str)) + "%"
    df_ann.loc["percentage"] = [
        (
            ((df_ann[("count", "HiiProp")][-1] / TOTAL_COUNT) * 100)
            .round(2)
            .astype(str)
        )
        + "%",
        (
            ((df_ann[("count", "LowProp")][-1] / TOTAL_COUNT) * 100)
            .round(2)
            .astype(str)
        )
        + "%",
        (
            ((df_ann[("count", "MedProp")][-1] / TOTAL_COUNT) * 100)
            .round(2)
            .astype(str)
        )
        + "%",
        (
            ((df_ann[("count", "Total")][-1] / TOTAL_COUNT) * 100)
            .round(2)
            .astype(str)
        )
        + "%",
        "",
    ]
    df_ann.to_csv(SAVE_PATH / "Cat-prop_pivot.csv")
    dfnaps = dfnaps[dfnaps['Mobile_No'].isin(df2tech_passed2['Mobile_No'])]
    # dfnaps = dfnaps[dfnaps['dec_reason']=='NOT DECLINED']
    x = dfnaps[(dfnaps['Category']=='CAT-D')&(dfnaps['riskband2']=='Hig')]
    y = df2tech_passed2[~df2tech_passed2['Mobile_No'].isin(x['Mobile_No'])]
    y1 = df2tech_passed2[df2tech_passed2['Mobile_No'].isin(x['Mobile_No'])]
    y['PAN_NO'].fillna('nan',inplace=True)
    y['Address'].fillna(y['City'],inplace=True)
    y1['PAN_NO'].fillna('nan',inplace=True)
    y1['Address'].fillna(y['City'],inplace=True)

    y.to_csv(SAVE_PATH / f"{DATA_SOURCE}_offers_to_load.csv",index=False)
    y1.to_csv(SAVE_PATH / f"{DATA_SOURCE}_offers_to_hold.csv",index=False)
