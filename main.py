import sqlite3
import pandas as pd
from datetime import datetime
from memory_profiler import profile

# CSV files paths
CLIENT_CSV_PATH = './client.csv'
SERVER_CSV_PATH = './server.csv'

# result table name to upload data
RESULT_TABEL_NAME = 'result_info'

# SQL scripts
SQL_CREATE_TABLE = f"""
            CREATE TABLE {RESULT_TABEL_NAME} (
            timestamp timestamp,
            player_id int,
            event_id int,
            error_id varchar(255),
            json_server varchar(255),
            json_client varchar(255)
            );"""

SQL_SELECT = "SELECT * FROM 'cheaters' WHERE player_id in {}"

# date to filter data from csv files YYYY-mm-dd format
FILTER_DATE = '2021-05-21'


class SQLiteClient:
    _DB_FILE = './cheaters.db'

    def __init__(self, db_file_path: str = None) -> None:
        db_use = db_file_path if db_file_path else self._DB_FILE
        self.__connection = sqlite3.connect(db_use)

    def get_data_as_dataframe(self, sql: str, **kwargs) -> pd.DataFrame:
        with self.__connection as conn:
            df = pd.read_sql(sql, conn, **kwargs)
        return df

    def create_table(self, sql: str) -> None:
        try:
            with self.__connection as conn:
                conn.execute(sql)
        except sqlite3.Error as e:
            print(e)

    def push_data_from_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        with self.__connection as conn:
            res = df.to_sql(table_name, conn, if_exists='append', index=False)
        return res


class CSVHandler:
    CHUNK_SIZE = 1000

    @staticmethod
    def date_filter(df: pd.DataFrame, date: str) -> pd.DataFrame:
        date = datetime.strptime(date, '%Y-%m-%d').date()
        df = df[df['timestamp'].dt.date == date]
        return df

    @staticmethod
    def timestamp_to_date(df: pd.DataFrame) -> pd.DataFrame:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        return df

    @staticmethod
    def get_data_from_csv_file(path_to_file: str, chunksize: int) -> pd.DataFrame:
        df = pd.DataFrame()
        try:
            data = pd.read_csv(path_to_file, chunksize=chunksize)
        except FileNotFoundError:
            return df
        return data

    def processing(self, df: pd.DataFrame, date: str) -> pd.DataFrame:
        df = self.timestamp_to_date(df)
        df = self.date_filter(df, date)
        return df

    def main(self, file_path: str, date: str) -> pd.DataFrame:
        df = self.get_data_from_csv_file(file_path, chunksize=self.CHUNK_SIZE)
        dfs = [self.processing(x, date) for x in df]
        return pd.concat(dfs)


class Processing:

    def __init__(self) -> None:
        self.sqlite_cli = SQLiteClient()
        self.csv_handler = CSVHandler()

    @staticmethod
    def filter_df(df: pd.DataFrame) -> pd.DataFrame:
        condition = df.ban_time - pd.to_timedelta(24, 'h') < df.timestamp_server
        result = df[~condition]
        return result

    @staticmethod
    def drop_columns(df: pd.DataFrame, columns_to_drop: list) -> pd.DataFrame:
        df.drop(columns_to_drop, inplace=True, axis=1)
        return df

    @staticmethod
    def rename_columns(df: pd.DataFrame, name_map: dict) -> pd.DataFrame:
        df.rename(columns=name_map, inplace=True)
        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        name_map = {
            'timestamp_server': 'timestamp',
            'description_client': 'json_client',
            'description_server': 'json_server',
        }
        df = self.drop_columns(df, ['timestamp_client', 'ban_time'])
        df = self.rename_columns(df, name_map)
        return df

    @profile()
    def main(self, client_csv_filepath: str, server_csv_filepath: str, date: str) -> int:
        print(f'started in {datetime.now()}')
        self.sqlite_cli.create_table(SQL_CREATE_TABLE)
        df_client = self.csv_handler.main(client_csv_filepath, date)
        df_server = self.csv_handler.main(server_csv_filepath, date)
        merged_df = pd.merge(df_client, df_server, how='inner', on='error_id', suffixes=('_client', '_server'))
        player_ids = tuple(merged_df.player_id)
        db_data_df = self.sqlite_cli.get_data_as_dataframe(SQL_SELECT.format(player_ids), parse_dates=['ban_time'])
        merged_df = pd.merge(merged_df, db_data_df, how='left', on='player_id')
        result_df = self.filter_df(merged_df)
        data_df = self.transform_data(result_df)
        response = self.sqlite_cli.push_data_from_dataframe(data_df, RESULT_TABEL_NAME)
        print(f'finished in {datetime.now()}')
        print(f'inserted rows: {response}')
        return response


processing = Processing()
processing.main(CLIENT_CSV_PATH, SERVER_CSV_PATH, FILTER_DATE)
