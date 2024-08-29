# src/analytics/analysis_9.py
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

def perform_analysis_9(unit_df,damages_df):
    result = unit_df.join(damages_df, "CRASH_ID",'left') \
         .filter(F.col("DAMAGED_PROPERTY").isNull()) \
         .withColumn('VEH_DMAG_SCL_1_ID', F.regexp_replace('VEH_DMAG_SCL_1_ID', '[^0-9]', '').cast(IntegerType())) \
         .filter((F.col("VEH_DMAG_SCL_1_ID") > 4) & (unit_df["FIN_RESP_TYPE_ID"].like("%INSURANCE%"))) \
         .select(F.count_distinct("CRASH_ID").alias("Count of Distinct Crashes where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance"))

    return result