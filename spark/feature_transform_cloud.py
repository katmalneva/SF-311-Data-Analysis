# SF311 Feature Transformation — Google Cloud Dataproc
#
# Reads from MongoDB 311_DataBase.historic_data (~8.3M docs),
# transforms features, builds a SparkML pipeline,
# and writes Spark ML features to MongoDB (dev) or GCS Parquet (full run).

import argparse
import os

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession

SOURCE_COLLECTION = "historic_data"
TARGET_COLLECTION = "features_ready"

DROP_COLS = [
    "_id",
    "Point",
    "point_geom",
    "Address",
    "Status Notes",
    "Media URL",
    "data_as_of",
    "data_loaded_at",
    "Closed",
    "Updated",
    "Opened",
    "Status",
]

CAT_COLS = [
    "Responsible Agency",
    "Category",
    "Request Type",
    "Neighborhood",
    "Police District",
    "Source",
]

NUMERIC_COLS = [
    "Latitude",
    "Longitude",
    "Supervisor District",
    "BOS_2012",
    "hour_of_day",
    "day_of_week",
    "month",
    "year",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SF311 feature transformation job",
    )
    parser.add_argument(
        "--mongo_uri",
        default=None,
        help="MongoDB Atlas connection URI",
    )
    parser.add_argument(
        "--database",
        default="311_DataBase",
        help="MongoDB database name",
    )
    parser.add_argument(
        "--dev", action="store_true", default=False,
        help="Dev mode: limit to 10k rows, write to features_ready_subset in MongoDB",
    )
    parser.add_argument(
        "--output_path",
        default="gs://dds-group-proj-spark/sparkml_features_rf/",
        help="GCS path for Parquet output (ignored in --dev mode)",
    )
    return parser.parse_args()


def resolve_mongo_uri(arg_uri: str) -> str:
    """Return URI from CLI arg"""
    if arg_uri:
        return arg_uri
    uri = os.environ.get("MONGODB_URI", "")
    if not uri:
        raise ValueError(
            "MongoDB URI not found. Pass --mongo_uri.",
        )
    return uri


def engineer_features(df: DataFrame) -> DataFrame:
    """Parse date strings, compute resolution_hours, extract time parts."""
    date_fmt = "MM/dd/yyyy hh:mm:ss a"
    df = (
        df.withColumn("opened_ts", F.to_timestamp("Opened", date_fmt))
        .withColumn("closed_ts", F.to_timestamp("Closed", date_fmt))
        .withColumn(
            "resolution_hours",
            (F.unix_timestamp("closed_ts") - F.unix_timestamp("opened_ts")) / 3600.0,
        )
        .withColumn("hour_of_day", F.hour("opened_ts"))
        .withColumn("day_of_week", F.dayofweek("opened_ts"))
        .withColumn("month", F.month("opened_ts"))
        .withColumn("year", F.year("opened_ts"))
        .drop("opened_ts", "closed_ts")
    )
    return df


def fill_nulls(df: DataFrame) -> DataFrame:
    str_nulls = [
        "Street", "Neighborhood", "Analysis Neighborhood",
        "Police District", "Source", "Request Details",
    ]
    num_nulls = ["Supervisor District", "BOS_2012", "Latitude", "Longitude"]
    df = df.fillna("Unknown", subset=str_nulls)
    df = df.fillna(0, subset=num_nulls)
    return df


def build_pipeline() -> Pipeline:
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        # From the docs:  A label indexer that maps a string column of labels to an ML column of label indices.
        # If the input column is numeric, we cast it to string and index the string values.
        # The indices are in [0, numLabels). By default, this is ordered by label frequencies
        # so the most frequent label gets index 0.
        for c in CAT_COLS
    ]
    idx_cols = [f"{c}_idx" for c in CAT_COLS]
    assembler = VectorAssembler(
        inputCols=NUMERIC_COLS + idx_cols,
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(stages=indexers + [assembler])


def main() -> None:
    args = parse_args()
    mongo_uri = resolve_mongo_uri(args.mongo_uri)

    builder = (
        SparkSession.builder.appName("SF311FeatureTransformCloud")
        .config("spark.sql.shuffle.partitions", "20" if args.dev else "200")
        .config("spark.mongodb.read.connection.uri", mongo_uri)
    )
    if args.dev:
        builder = builder.config("spark.mongodb.write.connection.uri", mongo_uri)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading {args.database}.{SOURCE_COLLECTION} ...")
    df = (
        spark.read.format("mongodb")
        .option("database", args.database)
        .option("collection", SOURCE_COLLECTION)
        .load()
    )

    if args.dev:
        print("[DEV] Limiting input to 10,000 rows")
        df = df.limit(10_000)

    df = engineer_features(df)
    df = df.filter(F.col("resolution_hours") > 0)
    df = df.drop(*[c for c in DROP_COLS if c in df.columns])
    df = fill_nulls(df)

    if args.dev:
        print(f"[DEV] Rows after feature engineering & filter: {df.count()}")

    print("Building and fitting ML pipeline ...")
    pipeline = build_pipeline()
    model = pipeline.fit(df)

    if args.dev:
        print("[DEV] Pipeline fitted successfully")

    features_df = model.transform(df)

    if args.dev:
        print(f"[DEV] Output rows: {features_df.count()}")

    output_df = features_df.select(
        "CaseID",
        vector_to_array("features").alias("features"),
        "resolution_hours",
    )

    if args.dev:
        print(f"[DEV] Writing to {args.database}.features_ready_subset ...")
        (
            output_df.write.format("mongodb")
            .option("database", args.database)
            .option("collection", "features_ready_subset")
            .mode("overwrite")
            .save()
        )
    else:
        print(f"Writing Parquet to {args.output_path} ...")
        output_df.write.mode("overwrite").parquet(args.output_path)

    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
