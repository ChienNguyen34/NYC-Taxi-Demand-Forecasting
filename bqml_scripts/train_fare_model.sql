-- bqml_scripts/train_fare_model.sql
CREATE OR REPLACE MODEL `nyc-taxi-project-477115.ml_models.fare_estimation_model`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['fare_amount']
) AS
SELECT
  *
FROM
  `nyc-taxi-project-477115.facts.fct_fare_prediction_training`;
