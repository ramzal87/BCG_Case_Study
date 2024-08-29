# src/analytics/analysis_4.py
from pyspark.sql import functions as F

def perform_analysis_4(primary_person_df, unit_df):
    result = unit_df.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"]).dropna(subset=["DRVR_LIC_CLS_ID","DRVR_LIC_TYPE_ID"]) \
    .filter((unit_df["UNIT_DESC_ID"] == "MOTOR VEHICLE") & (primary_person_df["DRVR_LIC_TYPE_ID"].isin(["DRIVER LICENSE","COMMERCIAL DRIVER LIC."])) & (~primary_person_df["DRVR_LIC_CLS_ID"].isin(["OTHER/OUT OF STATE","UNKNOWN","UNLICENSED"])) & (unit_df["VEH_HNR_FL"] == "Y")) \
    .select(F.count("CRASH_ID").alias("Number of Vehicles with driver having valid licences involved in hit and run"))

    return result
