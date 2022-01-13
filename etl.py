from datetime import timedelta, datetime
from urllib.error import HTTPError

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
    except HTTPError:
        return pd.DataFrame()
    else:
        return df


@task(name='transform')
def transform(covid_df: pd.DataFrame, variant_df: pd.DataFrame) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    if not covid_df.empty:
        if not variant_df.empty:
            logger.info("computing maximum variant from variant dataframe")
            max_variant_query = 'SELECT location, date, variant, MAX(num_sequences) AS max_sequences ' \
                                'FROM variant_df GROUP BY location, date'
            max_variant_df = pandasql.sqldf(max_variant_query)

            logger.info("adding extra columns variant and maximum num_sequences")
            covid_variant = covid_df.merge(max_variant_df)
            return covid_variant
        else:
            return covid_df
    else:
        raise RuntimeError(f'covid data: {covid_df.info()} variant data: {variant_df.info()}')


def db_connection():
    # In a better setup I would call like that:
    # 'postgresql://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}@localhost:5433/{os.environ['POSTGRES_DB']}'
    conn_string = 'postgresql://postgres:mysecretpassword@localhost:5433/postgres'
    return create_engine(conn_string)


@task(name='load')
def load(data: pd.DataFrame, populate_all=True) -> pd.DataFrame:
    logger = prefect.context.get("logger")

    logger.info('connecting to postgres')
    conn = db_connection()

    logger.info('populating postgres db')
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

        # the link was not working anymore by the time I ve submitted the task please replace with path to other dataset when possible!  # noqa
        variant_url = Parameter(
            'variant_url',
            default='https://github.com/owid/covid-19-data/blob/master/public/data/archived/variants/covid-variants.csv')  # noqa
        covid_df = extract(covid_url)
        variant_df = extract(variant_url)

        data_to_populate = transform(covid_df, variant_df)

        load(data_to_populate, True)

    flow.run_config = LocalRun()
    flow.register(project_name="covid-etl")
    flow.run()
