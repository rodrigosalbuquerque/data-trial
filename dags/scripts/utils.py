import pandas as pd
import us
from deep_translator import GoogleTranslator
from timezonefinder import TimezoneFinder

def drop_null_columns(df: pd.DataFrame):
    return df.dropna(axis=1, how='all')

def fill_missing_timezones(raw_df):
    tf = TimezoneFinder()
    null_timezone_df = raw_df[raw_df['time_zone'].isnull()]

    null_timezone_df['time_zone'] = null_timezone_df.apply(
        lambda row: tf.timezone_at(lng=row['longitude'], lat=row['latitude'])
        if pd.notna(row['latitude']) and pd.notna(row['longitude'])
        else None,
        axis=1
    )

    updated_df = raw_df.combine_first(null_timezone_df)

    return updated_df

def translate_text(text):
    try:
        translation = GoogleTranslator(source='auto', target='en').translate(text)
        print(f'Success translationg {text} to {translation}')
        return translation
    except Exception as e:
        print(f"Error translating '{text}': {e}")
        return text

def normalize_state(state_name):
    state = us.states.lookup(state_name)
    if state:
        print(f'Abbreaviating state name {state_name} to {state.abbr}')
        return state.abbr
    else:
        return state_name if len(state_name) == 2 else None

def ensure_datetime_mixed(dataframe, column_name):
    try:
        dataframe[column_name] = pd.to_datetime(
            dataframe[column_name],
            format='%m/%d/%Y %H:%M:%S',
            errors='coerce'
        )
    except:
        dataframe[column_name] = pd.to_datetime(
            dataframe[column_name],
            format='%m/%d/%Y %H:%M',
            errors='coerce'
        )
    return dataframe
