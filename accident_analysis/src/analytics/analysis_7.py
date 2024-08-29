# src/analytics/analysis_7.py
from pyspark.sql import functions as F
from pyspark.sql import Window

def perform_analysis_7(primary_person_df, unit_df):
    result = unit_df.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"]).dropna(subset=["VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID"]) \
        .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
        .count() \
        .withColumn("rank", F.row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(F.desc("count")))) \
        .filter(F.col("rank") == 1) \
        .select(F.col("VEH_BODY_STYL_ID").alias("Vehicle body style"), F.col("PRSN_ETHNICITY_ID").alias("Top ethnic user group"))

    return result