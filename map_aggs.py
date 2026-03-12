import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import folium
import pyspark.sql.functions as F
import pandas as pd
load_dotenv()

mongo_uri = os.getenv("MONGODB_URI")
db_name = '311_DataBase'
collection_name = 'historic_data'
full_uri = f"{mongo_uri}{db_name}.{collection_name}"

def create_spark_session():
    print("~~~~~~making spark session~~~~")
    ss = SparkSession.builder \
    .appName("MongoDBConnect") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.0.0") \
    .config("spark.mongodb.read.connection.uri", mongo_uri) \
    .config("spark.mongodb.read.database", db_name) \
    .config("spark.mongodb.read.collection", collection_name) \
    .config("spark.mongodb.write.connection.uri", mongo_uri) \
    .config("spark.mongodb.write.database", db_name) \
    .config("spark.mongodb.write.collection", collection_name) \
    .getOrCreate()
    return ss


def make_df(ss, desired_date):
    print("~~~~~making a dataframe from mongo db~~~~~")
    df = ss.read.format("mongodb") \
    .option("database", "311_DataBase") \
    .option("collection", "historic_data") \
    .load()
    filtered_df = df.select('Opened','Closed', 'Latitude', 'Longitude', 'Category', 'Neighborhood')\
	.filter(F.col('Opened').contains(desired_date))
    filtered_df.show()

    return filtered_df

def make_map(df):
    print("~~~~~making map~~~~~")
    df = df.toPandas()
    df.dropna(inplace=True)
    # Center map on SF
    m = folium.Map(location=[37.7749, -122.4194], zoom_start=13)

    # Add each ticket as a marker
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['Latitude'], row['Longitude']],
            radius=5,
            color='red',
            fill=True,
            fill_opacity=0.7,
            popup=folium.Popup(
                f"<b>{row['Category']}</b><br>{row['Neighborhood']}<br>{row['Opened']}<br>{row['Closed']}",
                max_width=200
            )
        ).add_to(m)

    m.save('sf_tickets.html')

if __name__ == "__main__":
    date = '02/10/2025'

    session = create_spark_session()
    spark_df = make_df(ss=session, desired_date=date)
    make_map(spark_df)

