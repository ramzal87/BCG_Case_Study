# src/analytics/analysis_2.py
from pyspark.sql import functions as F

def perform_analysis_2(unit_df):
    result = unit_df \
    .filter(unit_df["VEH_BODY_STYL_ID"].like("%MOTORCYCLE%")) \
    .select(F.count("CRASH_ID").alias("Number of two wheelers crashed"))
    
    return result
