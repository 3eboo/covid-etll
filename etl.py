from datetime import timedelta, datetime

import pandas
import pandas as pd
import pandasql
from prefect import task, Flow, Parameter, Task
from prefect.run_configs import LocalRun
from prefect.schedules import IntervalSchedule
from prefect.tasks import prefect
from sqlalchemy import create_engine


@task(name='extract', max_retries=2, retry_delay=timedelta(seconds=10))
def extract(url: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(url)
        # casting date column to datetime object.
        df['date'] = pd.to_datetime(df['date'])
    except Exception as e:
        raise RuntimeError(f'Failed to fetch data from {url} reason:{e}')
    else:
        return df


@task(name='transform')
def transform(covid_df: pd.DataFrame, variant_df: pd.DataFrame) -> pd.DataFrame:
    if not covid_df.empty and not variant_df.empty:
        logger = prefect.context.get("logger")
        logger.info("computing maximum variant from variant dataframe")
        max_variant_query = 'SELECT location, date, variant, MAX(num_sequences) AS max_sequences' \
                            'FROM variant_df GROUP BY location, date'
        max_variant_df = pandasql.sqldf(max_variant_query)

        logger.info("adding extra columns variant and maximum num_sequences")
        covid_variant = covid_df.merge(max_variant_df)

        logger.info("casting date column to datetime object")
        covid_variant['date'] = pd.to_datetime(covid_variant['date'])

        return covid_variant
    else:
        raise RuntimeError(f'covid data: {covid_df.info()} variant data: {variant_df.info()}')


def db_connection():
    conn_string = 'postgresql://postgres:mysecretpassword@localhost:5433/postgres'
    return create_engine(conn_string)


class Load(Task):
    def __init__(self, data: pandas.DataFrame, populate_all: bool = True):
        self.data = data
        self.db_engine = db_connection()
        self.populate_all = populate_all

    def run(self):
        if not self.populate_all:
            self.populate_partial()
        else:
            self.populate_partial()

    def populate_all(self):
        self.data.to_sql(name='covid_variant', con=self.db_engine, if_exists='replace')

    def populate_partial(self):
        pass


# Interval running the flow 2 times per day.
INTERVAL = IntervalSchedule(start_date=(datetime.utcnow() + timedelta(hours=1)), interval=timedelta(hours=12))

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    with Flow("covid-variants-etl", schedule=INTERVAL) as flow:
        covid_url = Parameter(
            'covid_url', default='https://covid.ourworldindata.org/data/owid-covid-data.csv')
        variant_url = Parameter(
            'variant_url',
            default='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/variants/covid-variants.csv')
        covid_df = extract(covid_url)
        variant_df = extract(variant_url)

        data_to_populate = transform(covid_df, variant_df)
        data = Parameter('data', default=data_to_populate)
        Load(data)
    flow.run_config = LocalRun(env={"SOME_VAR": "value"})
    flow.register(project_name="covid-etl")
    flow.run()
