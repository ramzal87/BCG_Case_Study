# src/analytics/analysis_8.py
from pyspark.sql import functions as F

def perform_analysis_8(primary_person_df,unit_df):
    result = unit_df.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"]).dropna(subset=["DRVR_ZIP"]) \
         .filter((unit_df["CONTRIB_FACTR_1_ID"] == "UNDER INFLUENCE - ALCOHOL") & (F.trim(primary_person_df['DRVR_ZIP']) !='')) \
         .groupBy(F.col("DRVR_ZIP").alias("Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash ")) \
         .count() \
         .orderBy("count", ascending=False) \
         .limit(5)

    return result