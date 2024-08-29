# src/main.py
from analytics import analysis_1, analysis_2, analysis_3, analysis_4, analysis_5
from analytics import analysis_6, analysis_7, analysis_8, analysis_9, analysis_10
from utils import get_spark_session, load_config,write_csv,read_csv

def main():
    # Initialize Spark session
    spark = get_spark_session()

    # Load configuration
    config = load_config("config/config.yml")

    # Load input data
    primary_person_df = read_csv(spark, config["input_paths"]["primary_person"])
    unit_df = read_csv(spark, config["input_paths"]["unit"])
    charges_df = read_csv(spark, config["input_paths"]["charges"])
    endorsements_df = read_csv(spark, config["input_paths"]["endorsements"])
    restrict_df = read_csv(spark, config["input_paths"]["restrict"])
    damages_df = read_csv(spark, config["input_paths"]["damages"])

    # Perform analyses
    result_1 = analysis_1.perform_analysis_1(primary_person_df)
    result_2 = analysis_2.perform_analysis_2(unit_df)
    result_3 = analysis_3.perform_analysis_3(primary_person_df, unit_df)
    result_4 = analysis_4.perform_analysis_4(primary_person_df, unit_df)
    result_5 = analysis_5.perform_analysis_5(primary_person_df, unit_df)
    result_6 = analysis_6.perform_analysis_6(primary_person_df, unit_df)
    result_7 = analysis_7.perform_analysis_7(primary_person_df, unit_df)
    result_8 = analysis_8.perform_analysis_8(primary_person_df, unit_df)
    result_9 = analysis_9.perform_analysis_9(unit_df, damages_df)
    result_10 = analysis_10.perform_analysis_10(primary_person_df, unit_df, charges_df)

    # Save results
    write_csv(result_1, config["output_paths"]["analysis_1"])
    write_csv(result_2, config["output_paths"]["analysis_2"])
    write_csv(result_3, config["output_paths"]["analysis_3"])
    write_csv(result_4, config["output_paths"]["analysis_4"])
    write_csv(result_5, config["output_paths"]["analysis_5"])
    write_csv(result_6, config["output_paths"]["analysis_6"])
    write_csv(result_7, config["output_paths"]["analysis_7"])
    write_csv(result_8, config["output_paths"]["analysis_8"])
    write_csv(result_9, config["output_paths"]["analysis_9"])
    write_csv(result_10, config["output_paths"]["analysis_10"])

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
