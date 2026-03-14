import os
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "311_DataBase"
COLLECTION_NAME = "historic_data"
LOCAL_PARQUET = "./311_data"
LOCAL_AGGS = "./aggregations"

spark = SparkSession.builder \
    .appName("311_Analysis") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()


def get_collection(mongo_uri: str, db_name: str, collection_name: str):
    client = MongoClient(mongo_uri)
    return client[db_name][collection_name]


def create_df(spark, db, collection):
    format1 = "MM/dd/yyyy hh:mm:ss a"
    format2 = "yyyy/MM/dd hh:mm:ss a"

    time_cols = {
        "Opened":         format1,
        "Closed":         format1,
        "Updated":        format1,
        "data_as_of":     format2,
        "data_loaded_at": format2,
    }

    df = spark.read.format("mongodb") \
        .option("database", db) \
        .option("collection", collection) \
        .option("pipeline", '[{"$sort": {"Opened": -1}}]') \
        .load()

    for ts_col, fmt in time_cols.items():
        df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col), fmt))

    df = df.filter(F.col("Neighborhood").isNotNull())
    return df


def save_parquet(df, path):
    df.write.mode("overwrite").parquet(path)
    print(f"Saved parquet to {path}")


def load_parquet(spark, path):
    return spark.read.parquet(path)


def resolution_time_cat(spark):
    return spark.sql("""
        SELECT Category,
               ROUND(AVG(datediff(Closed, Opened)), 1) AS avg_days_to_close,
               COUNT(CaseID) AS total_cases
        FROM cases
        WHERE Closed IS NOT NULL AND Opened IS NOT NULL
        GROUP BY Category
        ORDER BY avg_days_to_close DESC
    """)


def cases_by_neighborhood(spark):
    return spark.sql("""
        SELECT Neighborhood,
               COUNT(CaseID) AS total_cases,
               ROUND(AVG(datediff(Closed, Opened)), 1) AS avg_days_to_close,
               FIRST(Latitude)  AS Latitude,
               FIRST(Longitude) AS Longitude
        FROM cases
        WHERE Neighborhood IS NOT NULL
        GROUP BY Neighborhood
        ORDER BY total_cases DESC
    """)



def monthly_case_counts(spark):
    return spark.sql("""
        SELECT month, ROUND(AVG(total_cases), 1) AS avg_cases
        FROM (
            SELECT YEAR(Opened) AS year,
                   MONTH(Opened) AS month,
                   COUNT(CaseID) AS total_cases
            FROM cases
            WHERE Opened IS NOT NULL
            GROUP BY YEAR(Opened), MONTH(Opened)
        )
        GROUP BY month
        ORDER BY avg_cases DESC
    """)
    
def resolution_time_by_neighborhood(spark):
    return spark.sql("""
        SELECT Neighborhood,
               ROUND(AVG(datediff(Closed, Opened)), 1) AS avg_days_to_close,
               COUNT(CaseID) AS total_cases
        FROM cases
        WHERE Neighborhood IS NOT NULL
        GROUP BY Neighborhood
        ORDER BY avg_days_to_close DESC
    """)


if __name__ == "__main__":
    print("Creating DataFrame from MongoDB...")
    df = create_df(spark, DB_NAME, COLLECTION_NAME)
    df = load_parquet(spark, LOCAL_PARQUET)
    df.createOrReplaceTempView("cases")

    print("\n--- Resolution Time by Category ---")
    res_df = resolution_time_cat(spark)
    res_df.show(10, False)
    save_parquet(res_df, f"{LOCAL_AGGS}/resolution_time/")

    print("\n--- Cases by Neighborhood ---")
    nbr_df = cases_by_neighborhood(spark)
    nbr_df.show(10, False)

    print("\n--- Average Monthly Case Counts ---")
    monthly_df = monthly_case_counts(spark)
    monthly_df.show(24, False)
    
    print("\n--- Resolution Time by Neighborhood ---")
    res_neighborhood = resolution_time_by_neighborhood(spark)
    res_neighborhood.show(10, False)

    spark.stop()