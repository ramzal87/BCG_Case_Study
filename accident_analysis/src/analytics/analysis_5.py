# src/analytics/analysis_5.py
from pyspark.sql import functions as F

def perform_analysis_5(primary_person_df,unit_df):
    result = unit_df.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"]) \
    .filter(primary_person_df["PRSN_GNDR_ID"] != "FEMALE") \
    .groupBy("VEH_LIC_STATE_ID") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(1).select(F.col("VEH_LIC_STATE_ID").alias("State with highest number of accidents in which females are not involved"))

    return result
