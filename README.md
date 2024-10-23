# Clever Data Engineer Trial Project

## Goal of the Project:

At Clever, we are turning data into personalized and localized content so our readers can make well-informed decisions about any step of their real estate journey.

Please fork this repository and let us know when you've completed the project with a link to your fork.

Using the data set decribed below, and the Airflow-based data pipeline docker configuration, perform basic analysis for the chosen in `company_profiles_google_maps.csv`. There are basic errors on the DAG that you will need to fix for the pipeline to work properly. 

## Expectations
To perform this project you will need to:
* Perform transforms on the raw data and load them into a PostgreSQL database
* Be able to join datasets together in way for an analyst to be able to rank by a certain set of criteria (you can determine this criteria)
* Be able to filter the data by city or state so analysis can be performed by locality
* Given a locality, create a ranked list according to the criteria you’ve chosen

**Bonus:**
* Interesting additional analysis based on the underlying data
* An example could be Review Sentiment, common themes among ratings, complaints, etc.

## Dataset
Moving company data set (files can be found at 'dags/scripts/data_examples' folder)
* fmcsa_companies.csv
* fmcsa_company_snapshot.csv
* fmcsa_complaints.csv
* fmcsa_safer_data.csv
* company_profiles_google_maps.csv
* customer_reviews_google.csv


## Getting started
To get started with Airflow check the [getting started](docs/getting_started.md) documentation.

# Project Solution

## Overview
In this project, my goal was to automate a data pipeline proccess that could extract, transform and load (ETL) data 
from multiple datasets into a PostgreSQL database. The structure of the pipeline is:
- dag airflow orchestrates the execution of the etl proccess
- raw schema: stores raw data only with typing transformations
- curated schema: stores transformed, aggregated and refined data ready for analysis
- jupyter notebook contains example of analysis that could be created with input of the analyst
- sentiment analysis using afinn

## Next Steps
Some opportunities for improvements would be:
- Add a data lake step, where we could load raw and transformed data, leaving the database only for refined data
- Add dbt to transform and catalog data/tables
- Improve analysis made, adding more enriched algorithms or even creating graphs to showcase the data
