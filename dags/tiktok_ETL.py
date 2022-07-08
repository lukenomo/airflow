from airflow.decorators import dag, task, task_group
from airflow.providers.mongo.hooks.mongo import MongoHook
from sqlalchemy_utils.types.enriched_datetime.pendulum_date import pendulum

RAW_DATA_DIR = "/home/yan/airflow/include/data/raw/"
DATA_IN_PROCESSING_DIR = "/home/yan/airflow/include/data/in_processing/"
CLEAR_DIR = "/home/yan/airflow/include/data/clear/"
FILE_NAME = "tiktok"

args = {'provide_context': True}


@dag(dag_id="tiktok_ETL", tags=["tiktok"], schedule_interval=None, start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
     catchup=False, default_args=args)
def tiktok_ETL():
    def save(ti, dataframe, file_name=FILE_NAME, dir_name=DATA_IN_PROCESSING_DIR):
        import pandas as pd
        dataframe.to_csv(dir_name + file_name + '.csv')
        ti.xcom_push(file_name, dir_name + file_name + '.csv')

    def get_df(ti, file_name=FILE_NAME):
        import pandas as pd
        return pd.read_csv(ti.xcom_pull(key=file_name), index_col=0)

    @task(task_id="extract", retries=2)
    def extract(file_name=FILE_NAME, **kwargs):
        ti = kwargs['ti']
        ti.xcom_push(file_name + '_raw', RAW_DATA_DIR + file_name + '_raw.csv')

    @task(task_id="unset_rows", retries=2)
    def unset_rows(file_name=FILE_NAME, **kwargs):
        df = get_df(kwargs['ti'], file_name + '_raw')
        df.dropna(how="all", inplace=True)
        save(kwargs['ti'], df, file_name + '_proc')

    @task(task_id="unnecessary_symbols", retries=2)
    def unnecessary_symbols(file_name=FILE_NAME, **kwargs):
        df = get_df(kwargs['ti'], file_name + '_proc')
        df['content'].replace(to_replace="[^A-z0-9.,'!?:;\- ]", value='', regex=True, inplace=True)
        save(kwargs['ti'], df, file_name + '_proc')

    @task_group()
    def cleaning():
        unset_rws = unset_rows()
        unnecessary_sumb = unnecessary_symbols()
        unset_rws >> unnecessary_sumb

    @task(task_id="replace", retries=2)
    def replace(file_name=FILE_NAME, **kwargs):
        df = get_df(kwargs['ti'], file_name + '_proc')
        df.fillna('-', inplace=True)
        save(kwargs['ti'], df, file_name + '_proc')

    @task(task_id="sort", retries=2)
    def sort(file_name=FILE_NAME, **kwargs):
        df = get_df(kwargs['ti'], file_name + '_proc')
        df.sort_values(by=["at"], ascending=False, inplace=True)
        save(kwargs['ti'], df, file_name + '_clean', CLEAR_DIR)

    @task_group()
    def preprocessing():
        rep = replace()
        srt = sort()
        rep >> srt

    @task(task_id="load", retries=2)
    def load(file_name=FILE_NAME, **kwargs):
        import pandas as pd
        import json
        conn = MongoHook(conn_id="local_mongo")
        coll = conn.get_collection("clear_data", "admin")
        data = pd.read_csv(CLEAR_DIR + file_name + '_clean.csv', index_col=0)
        payload = json.loads(data.to_json(orient='records'))
        coll.delete_many({})
        coll.insert_many(payload)

    extract = extract()
    cleaning = cleaning()
    preprocessing = preprocessing()
    load = load()

    extract >> cleaning >> preprocessing >> load


tiktok_ETL = tiktok_ETL()