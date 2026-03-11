"""Print descriptive statistics for the feature Parquet dataset."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DescribeFeatures").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("gs://dds-group-proj-spark/sparkml_features_rf/")

print(f"\nTotal rows: {df.count():,}")
print(f"Columns: {df.columns}\n")

# Describe resolution_hours (the target)
print("resolution_hours")
df.select("resolution_hours").summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
).show()

# Expand the features array into named columns for describe
feature_names = [
    "latitude", "longitude", "is_business_hours", "is_weekend",
    "month_sin", "month_cos", "day_of_week_sin",
    "year", "neighborhood_idx", "category_idx",
    "source_idx", "responsible_agency_idx",
    "request_type_idx", "supervisor_district"
]

for i, name in enumerate(feature_names):
    df = df.withColumn(name, F.col("features")[i])

print("Feature statistics")
df.select(feature_names).summary(
    "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"
).show(truncate=False)

print("resolution_hours percentiles")
percentiles = df.select("resolution_hours").approxQuantile(
    "resolution_hours", [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01
)
labels = ["10%", "25%", "50%", "75%", "90%", "95%", "99%"]
for label, val in zip(labels, percentiles):
    print(f"  {label}: {val:,.2f} hours ({val/24:,.1f} days)")

spark.stop()
