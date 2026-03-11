
# **GCP CLI Instructions: Feature Transformation Pipeline**

Read data from the `historical_data` collection, transform features, and write to the `features_ready` collection in your MongoDB Atlas cluster.

---

**Step 1 – Create a Google Cloud Storage Bucket**

```bash
gcloud storage buckets create gs://dds-group-proj-spark \
  --location=us-central1 \
  --project=dds-group-proj
```

Upload your PySpark script to the bucket:

```bash
gcloud storage cp feature_transform_cloud.py gs://dds-group-proj-spark/
```

---

**Step 2 – Create a Dataproc Cluster**

I found that `e2-standard-4` machines are more available than `n1` instances:

```bash
gcloud dataproc clusters create sf311-cluster \
    --region=us-central1 \
    --master-machine-type=e2-standard-4 \
    --worker-machine-type=e2-standard-4 \
    --num-workers=2 \
    --image-version=2.1-debian11 \
    --project=dds-group-proj
```

---

**Step 3 – Submit in Dev Mode first (10k rows → MongoDB `features_ready_subset`)**

Use this to validate the pipeline end-to-end before running the full dataset. Writes ~10k docs to the `features_ready_subset` collection in MongoDB Atlas so the team can inspect the schema and values. Each doc should have a **14-element `features` array**.

Re-upload the script after any changes:

```bash
gcloud storage cp feature_transform_cloud.py gs://dds-group-proj-spark/
```

Then submit with `--dev`:

```bash
gcloud dataproc jobs submit pyspark gs://dds-group-proj-spark/feature_transform_cloud.py \
    --cluster=sf311-cluster \
    --region=us-central1 \
    --project=dds-group-proj \
    --properties=spark.jars.packages=org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    -- \
    --mongo_uri "$MONGODB_URI" \
    --database 311_DataBase \
    --dev
```

Check MongoDB for a `features_ready_subset` collection with ~10k docs. Confirm `features` array has **14 elements** and `resolution_hours` looks correct, then proceed to Step 4.

---

**Step 4 – Full Run (8.3M docs → GCS Parquet)**

Writes ML-ready 14-feature vectors as Parquet files to `gs://dds-group-proj-spark/sparkml_features_rf/`.

```bash
gcloud dataproc jobs submit pyspark gs://dds-group-proj-spark/feature_transform_cloud.py \
    --cluster=sf311-cluster \
    --region=us-central1 \
    --project=dds-group-proj \
    --properties=spark.jars.packages=org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    -- \
    --mongo_uri "$MONGODB_URI" \
    --database 311_DataBase \
    --output_path gs://dds-group-proj-spark/sparkml_features_rf/
```

Output Parquet files will appear at `gs://dds-group-proj-spark/sparkml_features_rf/` when the job completes.

---

**Step 5 – Train Model + Save to GCS**

Trains a SparkML `RandomForestRegressor` on the 14-feature Parquet files (time-based split: train ≤2021, val 2022, test ≥2023), evaluates it, then saves the trained model and metrics JSON to GCS.

Upload the script first:

```bash
gcloud storage cp train_model_cloud.py gs://dds-group-proj-spark/
```

**Dev run first** (5k rows per split — quick validation):

```bash
gcloud dataproc jobs submit pyspark gs://dds-group-proj-spark/train_model_cloud.py \
    --cluster=sf311-cluster \
    --region=us-central1 \
    --project=dds-group-proj \
    -- \
    --dev
```

Check printed RMSE/MAE/R^2 look reasonable, then run the full job.

**Full run** (trains on ≤2021 data, saves model to GCS):

```bash
gcloud dataproc jobs submit pyspark gs://dds-group-proj-spark/train_model_cloud.py \
    --cluster=sf311-cluster \
    --region=us-central1 \
    --project=dds-group-proj
```

Verify model saved:

```bash
gsutil ls gs://dds-group-proj-spark/models/sf311_rf_model/
gsutil cat gs://dds-group-proj-spark/models/sf311_rf_metrics.json
```

---

**Step 6 – Delete the cluster when done**

Delete immediately after the job completes:

```bash
gcloud dataproc clusters delete sf311-cluster --region=us-central1 --project=dds-group-proj
```
