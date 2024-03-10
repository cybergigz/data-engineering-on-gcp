from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils import timezone


default_args = {
    "start_date": timezone.datetime(2024, 3, 9),
    "owner": "Surasak Suksathit",
}
with DAG(
    "netflix_predictor_modeling",
    default_args=default_args,
    schedule="@daily",
    tags=["netflix", "bigquery"],
):

    train_model = BigQueryExecuteQueryOperator(
        task_id="train_model",
        sql="""
            create or replace model `pim_netflix.netflix_predictor`
            options(model_type='BOOSTED_TREE_CLASSIFIER') as
            select
                Age,
                Device as label
            from `pim_netflix.netflix`
        """,
        gcp_conn_id="my_gcp_conn",
        use_legacy_sql=False,
    )