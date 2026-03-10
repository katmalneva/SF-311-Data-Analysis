"""
Train a SparkML RandomForestRegressor on SF-311 features and save the
trained model to GCS.

Architecture:
    Parquet (GCS) → Spark trains RF → saved model → GCS
                                    → metrics JSON → GCS
"""

import argparse
import json
import subprocess
import tempfile

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Train RF and save model to GCS")
    parser.add_argument(
        "--input_path",
        default="gs://dds-group-proj-spark/sparkml_features_rf/",
        help="GCS path to Parquet feature files",
    )
    parser.add_argument(
        "--model_path",
        default="gs://dds-group-proj-spark/models/sf311_rf_model/",
        help="GCS path to save trained RF model",
    )
    parser.add_argument(
        "--metrics_path",
        default="gs://dds-group-proj-spark/models/sf311_rf_metrics.json",
        help="GCS path to write metrics JSON",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Limit each split to 5k rows for quick validation",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("SF311_RF_Train").getOrCreate()


def array_to_dense_vector(df, col_name: str, out_col: str):
    """Convert Array<Double> column (from Parquet) to MLlib DenseVector."""
    array_to_vec = F.udf(lambda a: Vectors.dense(a), VectorUDT())
    return df.withColumn(out_col, array_to_vec(F.col(col_name)))


def evaluate(predictions, label_col: str, pred_col: str) -> dict:
    metrics = {}
    for metric in ("rmse", "mae", "r2"):
        evaluator = RegressionEvaluator(
            labelCol=label_col, predictionCol=pred_col, metricName=metric
        )
        metrics[metric] = evaluator.evaluate(predictions)
    return metrics


def save_metrics_to_gcs(metrics: dict, gcs_path: str):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(metrics, f, indent=2)
        local_path = f.name
    subprocess.run(["gsutil", "cp", local_path, gcs_path], check=True)
    print(f"Metrics saved to {gcs_path}")


def main():
    args = parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading Parquet from {args.input_path}")
    df = spark.read.parquet(args.input_path)

    df = df.withColumn("year", F.col("features")[7].cast("int"))

    train_df = df.filter(F.col("year") <= 2021)
    val_df = df.filter(F.col("year") == 2022)
    test_df = df.filter(F.col("year") >= 2023)

    if args.dev:
        print("DEV MODE: sampling 5k rows per split (random, not limit)")
        train_size = max(1, train_df.count())
        val_size = max(1, val_df.count())
        test_size = max(1, test_df.count())
        train_df = train_df.sample(fraction=min(1.0, 5000 / train_size), seed=42)
        val_df = val_df.sample(fraction=min(1.0, 5000 / val_size), seed=42)
        test_df = test_df.sample(fraction=min(1.0, 5000 / test_size), seed=42)

    print(f"Split sizes — train: {train_df.count():,}  val: {val_df.count():,}  test: {test_df.count():,}")

    MAX_HOURS = 8760.0  # 1 year
    train_df = train_df.withColumn(
        "label", F.log1p(F.least(F.col("resolution_hours"), F.lit(MAX_HOURS)))
    )
    val_df = val_df.withColumn(
        "label", F.log1p(F.least(F.col("resolution_hours"), F.lit(MAX_HOURS)))
    )
    test_df = test_df.withColumn(
        "label", F.log1p(F.least(F.col("resolution_hours"), F.lit(MAX_HOURS)))
    )

    train_df = array_to_dense_vector(train_df, "features", "features_vec")
    val_df = array_to_dense_vector(val_df, "features", "features_vec")
    test_df = array_to_dense_vector(test_df, "features", "features_vec")

    rf = RandomForestRegressor(
        featuresCol="features_vec",
        labelCol="label",
        numTrees=100,
        maxDepth=10,
        seed=42,
    )
    print("Training RandomForestRegressor (numTrees=100, maxDepth=10, target=log1p(hours))…")
    model = rf.fit(train_df)

    val_preds = model.transform(val_df)
    test_preds = model.transform(test_df)

    val_metrics = evaluate(val_preds, "label", "prediction")
    test_metrics = evaluate(test_preds, "label", "prediction")

    print("\n── Validation metrics (log1p space) ──")
    for k, v in val_metrics.items():
        print(f"  {k.upper()}: {v:.4f}")

    print("\n── Test metrics (log1p space) ──")
    for k, v in test_metrics.items():
        print(f"  {k.upper()}: {v:.4f}")

    all_metrics = {"validation": val_metrics, "test": test_metrics}

    model.write().overwrite().save(args.model_path)
    print(f"\nModel saved to {args.model_path}")

    save_metrics_to_gcs(all_metrics, args.metrics_path)

    spark.stop()


if __name__ == "__main__":
    main()
