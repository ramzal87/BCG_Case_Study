# src/analytics/analysis_1.py
from pyspark.sql import functions as F

def perform_analysis_1(primary_person_df):
    result = primary_person_df \
    .filter((F.col("PRSN_GNDR_ID") == "MALE") & (primary_person_df["PRSN_INJRY_SEV_ID"] == 'KILLED')).groupBy(F.col("CRASH_ID")) \
    .count().filter(F.col("count") > 2).select(F.count("CRASH_ID").alias('Number of crashes in which number of males killed are greater than 2'))
    
    return result