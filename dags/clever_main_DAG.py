from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from scripts.clever_main_pipeline import customer_reviews_google_to_raw, company_profiles_google_maps_to_raw, \
    company_reviews_to_curated, fmcsa_companies_to_raw, fmcsa_complaints_to_raw, fmcsa_company_snapshot_to_raw, \
    fmcsa_safer_data_to_raw

default_args = {
    "owner": "alec.ventura",
    "start_date": datetime(2024, 10, 1),
}

with DAG("clever_main_DAG", default_args=default_args, catchup=False, schedule_interval='20 0 * * *', max_active_runs=1) as dag:

    start_task = EmptyOperator(task_id='start_task', dag=dag)

    raw_company_profiles_google_maps_task = PythonOperator(
        task_id='company_profiles_google_maps_to_raw',
        python_callable=company_profiles_google_maps_to_raw,
        dag=dag,
        execution_timeout=timedelta(minutes=5)
    )

    raw_customer_reviews_google_task = PythonOperator(
        task_id='customer_reviews_google_to_raw',
        python_callable=customer_reviews_google_to_raw,
        dag=dag,
        execution_timeout=timedelta(minutes=5)
    )

    curated_company_reviews_task = PythonOperator(
        task_id='company_reviews_to_curated',
        python_callable=company_reviews_to_curated,
        dag=dag,
        execution_timeout=timedelta(minutes=5)
    )

    raw_fmcsa_companies_task = PythonOperator(
        task_id='fmcsa_companies_to_raw',
        python_callable=fmcsa_companies_to_raw,
        dag=dag,
        execution_timeout=timedelta(minutes=5)
    )

    raw_fmcsa_complaints_task = PythonOperator(
        task_id='fmcsa_complaints_to_raw',
        python_callable=fmcsa_complaints_to_raw,
        dag=dag,
        execution_timeout=timedelta(minutes=5)
    )

    raw_fmcsa_company_snapshot_task = PythonOperator(
        task_id='fmcsa_company_snapshot_to_raw',
        python_callable=fmcsa_company_snapshot_to_raw,
        dag=dag,
        execution_timeout=timedelta(minutes=5)
    )

    raw_fmcsa_safer_data_task = PythonOperator(
        task_id='fmcsa_safer_data_to_raw',
        python_callable=fmcsa_safer_data_to_raw,
        dag=dag,
        execution_timeout=timedelta(minutes=5)
    )

    finish_task = EmptyOperator(task_id='finish_task', dag=dag)

    start_task >> [raw_company_profiles_google_maps_task, raw_customer_reviews_google_task, raw_fmcsa_companies_task,
                   raw_fmcsa_complaints_task, raw_fmcsa_company_snapshot_task, raw_fmcsa_safer_data_task]
    [raw_company_profiles_google_maps_task, raw_customer_reviews_google_task] >> curated_company_reviews_task
    [raw_fmcsa_companies_task, raw_fmcsa_complaints_task, raw_fmcsa_company_snapshot_task, raw_fmcsa_safer_data_task] >> finish_task
    curated_company_reviews_task >> finish_task
