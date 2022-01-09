from datetime import timedelta

import pandas as pd
import pandasql
from prefect import task, Task, Flow, Parameter
from prefect.schedules import IntervalSchedule
from pymongo import MongoClient


@staticmethod
def get_db_connection():
    client = MongoClient('mongodb://localhost:27017/')

    return client["mydatabase"]


@task(name='extract', max_retries=2, retry_delay=timedelta(seconds=10))
def extract(url: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(url)
    except Exception as e:
        raise RuntimeError(f'Failed to fetch data from {url} reason:{e}')
    else:
        return df


@task(name='transform')
def transform(covid_df: pd.DataFrame, variant_df: pd.DataFrame) -> pd.DataFrame:
    if not covid_df.empty and not variant_df.empty:
        # most frequent variant per key (location+date) from variant dataframe
        max_variant_query = 'SELECT location, date, variant, MAX(num_sequences) ' \
                            'FROM variant_df GROUP BY location, date'
        max_variant_df = pandasql.sqldf(max_variant_query)

        # joining most frequent variant to dataset
        join_query = 'SELECT * FROM covid_df INNER JOIN max_variant_df ' \
                     'ON covid_df.date=max_variant_df.date and covid_df.location=max_variant_df.location'
        data_with_variant_columns = pandasql.sqldf(join_query)

        # convert date column to datetime object.
        data_with_variant_columns['date'] = pd.to_datetime(data_with_variant_columns['date'])
        return data_with_variant_columns
    else:
        raise RuntimeError('data frame is empty')


class PopulateTask(Task):
    def __init__(self, target: pd.DataFrame):
        self.target = target
        self.db = get_db_connection()

    def run(self, data: pd.DataFrame):
        covid_coll = self.db['covid']
        # covid_coll
        pass


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    with Flow("covid-variants-etl", schedule=IntervalSchedule(interval=timedelta(hours=24))) as flow:
        covid_url = Parameter(
            'covid_url', default='https://covid.ourworldindata.org/data/owid-covid-data.csv')
        variant_url = Parameter(
            'variant_url', default='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/variants/covid-variants.csv')
        covid_df = extract(covid_url)
        variant_df = extract(variant_url)

        data_to_populate = transform(covid_df, variant_df)

        PopulateTask(data_to_populate)

    flow.run()
    flow.register(project_name="covid-etl")
