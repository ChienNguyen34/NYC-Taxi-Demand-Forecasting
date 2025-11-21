-- bqml_scripts/train_fare_model.sql
CREATE OR REPLACE MODEL `{{ var.gcp_project_id }}.ml_models.fare_estimation_model`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['fare_amount']
) AS
SELECT
  *
FROM
  `{{ var.gcp_project_id }}.facts.fct_fare_prediction_training`;
