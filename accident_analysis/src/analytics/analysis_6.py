# src/analytics/analysis_6.py
from pyspark.sql import functions as F

def perform_analysis_6(primary_person_df, unit_df):
    result = unit_df.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"]) \
          .filter((primary_person_df["TOT_INJRY_CNT"] > 0) | (primary_person_df["DEATH_CNT"] > 0)) \
          .groupBy(F.col("VEH_MAKE_ID").alias("Top 3rd to 5th Vehicle Makes that contribute to a largest number of injuries including death")) \
          .count() \
          .orderBy("count", ascending=False) \
          .limit(5).offset(2)

    return result