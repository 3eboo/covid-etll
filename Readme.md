# Simple ETL application for Popcore technical challenge  #

- - - - 

**Technologies used and why?**

* ETL Orchestration
    * Prefect: we need orchestrator to fetch data periodically (daily based on the information from source) to get
      updates. Prefect is used to build data workflows with modern functional approach overcoming problems of other data
      pipeline engines and orchestrators, plus providing out of the box monitoring to our flow.
* Data extraction and transformation
    * Pandas: since the datasets' size is small (around 50 mb) and in memory processing is possible in this case ,
      pandas is very suitable to run on a single instance, and we don't need to have the overhead of installation and
      setup that comes with other data distributed processing systems like spark or dask.
* Loading and database
    * Postgres: Looking into the nature of the data, it is an OLAP non-normalised kind of data where ideally stored in a
      no-sql db, but however in our case key-value is one to one ( i.e no column is of type collection/dict) so it is
      feasible to store it in a postgres db. which we can leverage the features of using SQLAlchemy to enable data
      quality/availability measuring.

### Remarks

* In usual data use-cases, different OLTPs database becomes the source of our OLAP, unlike in this case the pipeline is
  reversed somehow.
* For setup and deployment, ideally I would containerize everything even the flow code, as prefect's server and agent
  are already containerized I ve tried to dockerize task (code + postgres) as well, you can check dockerfile and compose
  that I tried to use in that case, this for some reason didn't work! However, running
  with [local agent](https://docs.prefect.io/api/latest/run_configs.html#localrun) is the methodology I ended up using.

* Additionally, the owners of owid/covid-19-data repo ve removed the variant dataset that I was using to do the
  aggregate!
  However the aggrtegation part of application works so if you have any version of variants' dataset downloaded you can
  test with, just change the path of `variant_url` parameter to its path!

### Project setup and Requirements to run our prefect flow:

you should have docker and docker-compose installed and started. Since prefect agent and service will run on container
and your code will live locally in which you can interact with prefects backend and ui using .

* clone and go to project dir, then install project requirements

```bash
pip install -r requirements.txt
```

* start docker-compose by running:

```bash
prefect server start
```

make sure running with server backend

```bash
prefect backend server
```

create project

```bash
prefect create project "covid-etl"
```

start local agent

```bash
prefect agent local start
```

* The application use postgres db to store transformed data, for that we need postgres container running with port
  listening to 5433 since prefect has its own postgres db using the default port.

```bash
docker run --name some-postgres -p 5433:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
```

* Then you can run the application and monitor the flow on http://localhost:8080/default when running

```bash
python3 etl.py
```

Happy review and looking forward to meet you guys ;) 