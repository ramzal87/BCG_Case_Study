# src/analytics/analysis_3.py
def perform_analysis_3(primary_person_df, unit_df):
    result = unit_df.join(primary_person_df,["CRASH_ID", "UNIT_NBR"]).dropna(subset="VEH_MAKE_ID") \
    .filter((primary_person_df["PRSN_TYPE_ID"] == 'DRIVER') & (primary_person_df["PRSN_INJRY_SEV_ID"] == 'KILLED') & (primary_person_df["PRSN_AIRBAG_ID"] == "NOT DEPLOYED") ) \
    .groupBy(unit_df["VEH_MAKE_ID"].alias("Vehicle Make")) \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(5)
    
    result = result.select(result["Vehicle Make"].alias("Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy"),result['count'])
    
    return result