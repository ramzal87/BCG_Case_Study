# src/utils.py
import yaml
from pyspark.sql import SparkSession

def get_spark_session(app_name="AccidentAnalysis"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_config(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def read_csv(spark, path):
    return spark.read.option("header", "true").option("inferSchema", "true").option("nullValue", "NA").csv(path)

def write_csv(df, path):
    df.write.mode("overwrite").csv(path, header=True)
