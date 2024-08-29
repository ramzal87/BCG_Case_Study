# src/analytics/analysis_10.py
from pyspark.sql import functions as F

def perform_analysis_10(primary_person_df, unit_df, charges_df):
    top_10_colors = unit_df.dropna(subset=["VEH_COLOR_ID"]) \
                .groupBy("VEH_COLOR_ID").count() \
                .orderBy("count", ascending=False) \
                .limit(10).select('VEH_COLOR_ID')
                
    top_25_states = unit_df.join(charges_df, ["CRASH_ID", "UNIT_NBR"]).dropna(subset=["VEH_LIC_STATE_ID"]) \
                .groupBy("VEH_LIC_STATE_ID").count() \
                .orderBy("count", ascending=False) \
                .limit(25).select('VEH_LIC_STATE_ID')
    
    result = unit_df.join(charges_df, ["CRASH_ID", "UNIT_NBR"]).join(primary_person_df,["CRASH_ID", "UNIT_NBR"]) \
        .filter(charges_df["CHARGE"].like("%SPEED%")) \
        .filter((unit_df["VEH_BODY_STYL_ID"].isin(["PASSENGER CAR, 4-DOOR","PASSENGER CAR, 2-DOOR"])) & (primary_person_df["DRVR_LIC_TYPE_ID"].isin(["DRIVER LICENSE","COMMERCIAL DRIVER LIC."]))) \
        .join(top_10_colors,["VEH_COLOR_ID"]) \
        .join(top_25_states,["VEH_LIC_STATE_ID"]) \
        .groupBy(F.col("VEH_MAKE_ID").alias("the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences ")) \
        .count() \
        .orderBy("count", ascending=False) \
        .limit(5)                    
            
    return result
