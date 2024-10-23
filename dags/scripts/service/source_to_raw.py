import pandas as pd

from scripts.utils import drop_null_columns, fill_missing_timezones, ensure_datetime_mixed

file_path = 'dags/scripts/data_examples/'

def company_profiles_google_maps(file_name):
    raw_df = pd.read_csv(
        f'{file_path}/{file_name}',
        escapechar='\\'
    )

    raw_df = drop_null_columns(raw_df)
    raw_df['postal_code'] = pd.to_numeric(raw_df['postal_code'], errors='coerce').astype('Int64').replace({pd.NA: None})
    raw_df['country_code'] = raw_df['country_code'].fillna('US')
    raw_df = fill_missing_timezones(raw_df)
    raw_df['reviews'] = raw_df['reviews'].fillna(0).astype(int)
    raw_df['verified'] = raw_df['verified'].fillna(False)
    raw_df['rating'] = raw_df['rating'].apply(lambda x: f"{x:.1f}")

    return raw_df


def customer_reviews_google(file_name):
    raw_df = pd.read_csv(
        f'{file_path}/{file_name}',
        escapechar='\\'
    )

    raw_df['reviews'] = raw_df['reviews'].fillna(0).astype(int)
    raw_df['rating'] = raw_df['rating'].apply(lambda x: f"{x:.1f}")
    raw_df['review_rating'] = raw_df['review_rating'].apply(lambda x: f"{x:.1f}")
    raw_df['author_id'] = raw_df['author_id'].astype(float).apply(lambda x: f'{x:.0f}')
    raw_df['author_reviews_count'] = raw_df['author_reviews_count'].fillna(0)
    raw_df = ensure_datetime_mixed(raw_df, 'review_datetime_utc')
    raw_df = ensure_datetime_mixed(raw_df, 'owner_answer_timestamp_datetime_utc')

    return raw_df


def fmcsa_companies(file_name):
    raw_df = pd.read_csv(
        f'{file_path}/{file_name}'
    )

    raw_df['date_created'] = (pd.to_datetime(raw_df['date_created'])).dt.date
    raw_df['date_updated'] = (pd.to_datetime(raw_df['date_updated'])).dt.date

    raw_df = raw_df[['usdot_num', 'user_created', 'date_created', 'user_updated', 'date_updated', 'company_name',
                     'city', 'state', 'total_complaints_2021', 'total_complaints_2022', 'total_complaints_2023',
                     'company_type']]

    return raw_df

def fmcsa_company_snapshot(file_name):
    raw_df = pd.read_csv(
        f'{file_path}/{file_name}'
    )

    raw_df['date_created'] = (pd.to_datetime(raw_df['date_created'])).dt.date
    raw_df['date_updated'] = (pd.to_datetime(raw_df['date_updated'])).dt.date

    raw_df = raw_df[
        ['usdot_num', 'user_created', 'date_created', 'user_updated', 'date_updated', 'company_name', 'mc_num',
         'registered_address', 'safety_review_date', 'num_of_trucks', 'num_of_tractors', 'num_of_trailers',
         'hhg_authorization', 'total_complaints_2021', 'total_complaints_2022', 'total_complaints_2023']]

    return raw_df

def fmcsa_complaints(file_name):
    raw_df = pd.read_csv(
        f'{file_path}/{file_name}'
    )

    raw_df['date_created'] = (pd.to_datetime(raw_df['date_created'])).dt.date
    raw_df.rename(columns={'id': 'complaint_id'}, inplace=True)

    raw_df = raw_df[
        ['usdot_num', 'complaint_id', 'user_created', 'date_created', 'complaint_category', 'complaint_year',
         'complaint_count']]

    return raw_df

def fmcsa_safer_data(file_name):
    raw_df = pd.read_csv(
        f'{file_path}/{file_name}'
    )

    raw_df.drop(columns=['fmcsa_link', 'state_carrier_id_number'], inplace=True)
    raw_df['date_created'] = (pd.to_datetime(raw_df['date_created'])).dt.date
    raw_df['date_updated'] = (pd.to_datetime(raw_df['date_updated'])).dt.date

    return raw_df
