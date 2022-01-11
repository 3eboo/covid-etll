from datetime import timedelta, datetime

import pandas as pd
import pandasql
from prefect import task, Flow, Parameter
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


@task(name='load')
def load(data: pd.DataFrame, populate_all=True) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info('connecting to postgres')
    conn = db_connection()
    logger.info('populating db')
    if populate_all:
        data.to_sql(name='covid_variant', con=conn, if_exists='replace')
        conn.execute('ALTER TABLE covid_variant ADD PRIMARY KEY (location, date);')
    else:
        data.to_sql(name='covid_variant', con=conn, if_exists='append')


# Interval running the flow 2 times per day.
INTERVAL = IntervalSchedule(start_date=(datetime.utcnow() + timedelta(hours=1)), interval=timedelta(hours=12))

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    with Flow("covid-variants-etl", schedule=INTERVAL) as flow:
        covid_url = Parameter(
            'covid_url', default='https://covid.ourworldindata.org/data/owid-covid-data.csv')

        # the link was not working anymore by the time I ve submitted the task please replace with path to other dataset when possible!
        variant_url = Parameter(
            'variant_url',
            default='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/variants/covid-variants.csv')
        covid_df = extract(covid_url)
        variant_df = extract(variant_url)

        data_to_populate = transform(covid_df, variant_df)
        load(data_to_populate, True)
    flow.run_config = LocalRun()
    flow.register(project_name="covid-etl")
    flow.run()
