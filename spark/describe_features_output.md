
# SF-311 Feature Dataset Statistics

**Source:** `gs://dds-group-proj-spark/sparkml_features_rf/`
**Total rows:** 8,188,962
**Columns:** `CaseID`, `features` (14-element array), `resolution_hours`

---

## Target Variable: `resolution_hours`

| Statistic | Value |
|-----------|-------|
| Count     | 8,188,962 |
| Mean      | 776.60 hours (32.4 days) |
| Std Dev   | 4,229.38 hours (176.2 days) |
| Min       | 0.0003 hours (1 second) |
| 25%       | 2.99 hours (0.1 days) |
| 50%       | 23.73 hours (1.0 days) |
| 75%       | 124.64 hours (5.2 days) |
| Max       | 131,140.94 hours (5,464.2 days / ~15 years) |

### Extended Percentiles

| Percentile | Hours | Days |
|------------|-------|------|
| 10%  | 0.73 | 0.0 |
| 25%  | 2.98 | 0.1 |
| 50%  | 23.57 | 1.0 |
| 75%  | 122.52 | 5.1 |
| 90%  | 721.30 | 30.1 |
| 95%  | 2,540.96 | 105.9 |
| 99%  | 131,140.94 | 5,464.2 |

---

## Feature Statistics (14 features)

| Feature | Mean | Std Dev | Min | 25% | 50% | 75% | Max |
|---------|------|---------|-----|-----|-----|-----|-----|
| latitude | 35.21 | 9.49 | 0.0 | 37.74 | 37.77 | 37.78 | 38.68 |
| longitude | -114.14 | 30.76 | -141.22 | -122.44 | -122.42 | -122.41 | 0.0 |
| is_business_hours | 5.94 | 3.20 | 0.0 | 3.0 | 6.0 | 9.0 | 11.0 |
| is_weekend | 4.72 | 3.72 | 0.0 | 1.0 | 5.0 | 8.0 | 11.0 |
| month_sin | 12.62 | 4.54 | 0.0 | 9.0 | 12.0 | 16.0 | 23.0 |
| month_cos | 3.97 | 1.89 | 1.0 | 2.0 | 4.0 | 6.0 | 7.0 |
| day_of_week_sin | 6.56 | 3.45 | 1.0 | 4.0 | 7.0 | 10.0 | 12.0 |
| year | 2019.22 | 4.47 | 2008.0 | 2016.0 | 2020.0 | 2023.0 | 2026.0 |
| neighborhood_idx | 15.05 | 32.41 | 0.0 | 0.0 | 3.0 | 14.0 | 614.0 |
| category_idx | 6.17 | 10.44 | 0.0 | 0.0 | 1.0 | 8.0 | 145.0 |
| source_idx | 36.69 | 113.52 | 0.0 | 2.0 | 10.0 | 38.0 | 2,263.0 |
| responsible_agency_idx | 34.46 | 40.62 | 0.0 | 4.0 | 20.0 | 49.0 | 235.0 |
| request_type_idx | 3.92 | 3.06 | 0.0 | 1.0 | 4.0 | 6.0 | 12.0 |
| supervisor_district | 1.05 | 1.13 | 0.0 | 0.0 | 1.0 | 2.0 | 11.0 |

---

## Model Metrics (Full Run)

Trained: `RandomForestRegressor(numTrees=100, maxDepth=10)`
Target: `log1p(resolution_hours)` with 8,760h (1-year) clip
Split: train ≤2021 (5.1M), val 2022 (655K), test ≥2023 (2.4M)

| Metric | Validation | Test |
|--------|-----------|------|
| RMSE   | 1.7968 | 1.9650 |
| MAE    | 1.3641 | 1.5842 |
| R²     | 0.3274 | 0.1639 |

Model saved to: `gs://dds-group-proj-spark/models/sf311_rf_model/`
Metrics saved to: `gs://dds-group-proj-spark/models/sf311_rf_metrics.json`


DEV DATASET:
── Validation metrics (log1p space) ──
  RMSE: 1.8200
  MAE: 1.4125
  R2: 0.3169

── Test metrics (log1p space) ──
  RMSE: 2.0074
  MAE: 1.6408
  R2: 0.1213



FULL DATASET:
── Validation metrics (log1p space) ──
  RMSE: 1.7895
  MAE: 1.3585
  R2: 0.3329

── Test metrics (log1p space) ──
  RMSE: 1.9637
  MAE: 1.5813
  R2: 0.1650


IDEA:
look into binning predictions instead because its hard to really pinpoint the hours 
