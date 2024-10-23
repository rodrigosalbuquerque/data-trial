from scripts.postgres_helper import upload_overwrite_table

from scripts.service.raw_to_curated import company_reviews
from scripts.service.source_to_raw import customer_reviews_google, company_profiles_google_maps
from scripts.service.source_to_raw import fmcsa_companies, fmcsa_company_snapshot, fmcsa_complaints, fmcsa_safer_data


def upload_to_postgres(df, table_name, schema='raw'):
    full_table_name = f'{schema}.{table_name}'
    upload_overwrite_table(df, full_table_name)

def company_profiles_google_maps_to_raw():
    file_name = 'company_profiles_google_maps.csv'
    table_name = file_name.split('.')[0]
    company_profiles_google_maps_raw_df = company_profiles_google_maps(file_name)
    upload_to_postgres(company_profiles_google_maps_raw_df, table_name, 'raw')

def customer_reviews_google_to_raw():
    file_name = 'customer_reviews_google.csv'
    table_name = file_name.split('.')[0]
    customer_reviews_google_raw_df = customer_reviews_google(file_name)
    upload_to_postgres(customer_reviews_google_raw_df, table_name, 'raw')

def company_reviews_to_curated():
    table_name = 'company_reviews'
    company_profiles_name = 'company_profiles_google_maps.csv'
    customer_reviews_name = 'customer_reviews_google.csv'
    company_reviews_curated_df = company_reviews(company_profiles_name, customer_reviews_name)
    upload_to_postgres(company_reviews_curated_df, table_name, 'curated')

def fmcsa_companies_to_raw():
    file_name = 'fmcsa_companies.csv'
    table_name = file_name.split('.')[0]
    fmcsa_companies_df = fmcsa_companies(file_name=file_name)
    upload_to_postgres(fmcsa_companies_df, table_name, 'raw')

def fmcsa_company_snapshot_to_raw():
    file_name = 'fmcsa_company_snapshot.csv'
    table_name = file_name.split('.')[0]
    fmcsa_company_snapshot_df = fmcsa_company_snapshot(file_name=file_name)
    upload_to_postgres(fmcsa_company_snapshot_df, table_name, 'raw')

def fmcsa_complaints_to_raw():
    file_name = 'fmcsa_complaints.csv'
    table_name = file_name.split('.')[0]
    fmcsa_complaints_df = fmcsa_complaints(file_name=file_name)
    upload_to_postgres(fmcsa_complaints_df, table_name, 'raw')

def fmcsa_safer_data_to_raw():
    file_name = 'fmcsa_safer_data.csv'
    table_name = file_name.split('.')[0]
    fmcsa_safer_data_df = fmcsa_safer_data(file_name=file_name)
    upload_to_postgres(fmcsa_safer_data_df, table_name, 'raw')
