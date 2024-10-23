import numpy as np
import pandas as pd

from scripts.utils import normalize_state, translate_text

from scripts.service.source_to_raw import company_profiles_google_maps, customer_reviews_google


def transform_company_profiles_data(dataframe):
    dataframe.rename(columns={
        'name': 'company_name',
        'rating': 'company_rating',
        'reviews': 'amount_of_reviews'
    }, inplace=True)

    dataframe = dataframe[dataframe['verified'] == True]
    dataframe = dataframe[dataframe['business_status'] == 'OPERATIONAL']
    dataframe['us_state'] = dataframe['us_state'].apply(normalize_state)

    df_company_profiles = dataframe[['google_id', 'place_id', 'company_name', 'type', 'city', 'state',
                                     'country_code', 'company_rating', 'amount_of_reviews']]

    return df_company_profiles

def transform_customer_reviews_data(dataframe):
    dataframe.rename(columns={
        'name': 'company_name',
        'rating': 'company_rating',
        'reviews': 'amount_of_reviews'
    }, inplace=True)

    dataframe['review_date'] = dataframe['review_datetime_utc'].dt.date
    dataframe['owner_answer_date'] = dataframe['owner_answer_timestamp_datetime_utc'].dt.date
    dataframe['amount_of_reviews'] = dataframe['amount_of_reviews'].astype(int)

    df_customer_reviews = dataframe[['google_id', 'place_id', 'company_name',  'company_rating', 'amount_of_reviews',
                                     'review_text', 'review_rating', 'review_date', 'owner_answer', 'owner_answer_date']]

    return df_customer_reviews

def company_reviews(company_profiles_name, customer_reviews_name):
    df_company_profiles = company_profiles_google_maps(company_profiles_name)
    df_customer_reviews = customer_reviews_google(customer_reviews_name)

    df_transformed_company_profiles = transform_company_profiles_data(df_company_profiles)
    df_transformed_customer_reviews = transform_customer_reviews_data(df_customer_reviews)

    df_merged = pd.merge(
        df_transformed_company_profiles,
        df_transformed_customer_reviews,
        how='left',
        on=['google_id', 'place_id', 'company_name', 'company_rating']
    )

    df_merged['amount_of_reviews'] = np.where(
        df_merged['amount_of_reviews_x'] != 0,
        df_merged['amount_of_reviews_x'],
        np.where(
            df_merged['amount_of_reviews_y'] != 0,
            df_merged['amount_of_reviews_y'],
            0
        )
    )
    df_merged['amount_of_reviews'] = df_merged['amount_of_reviews'].fillna(0).astype(int)
    df_merged['review_text'] = df_merged['review_text'].fillna('')

    df_merged = df_merged[['place_id', 'company_name', 'type', 'city', 'state', 'country_code', 'company_rating',
                           'amount_of_reviews', 'review_text', 'review_rating', 'review_date', 'owner_answer',
                           'owner_answer_date']]

    return df_merged
