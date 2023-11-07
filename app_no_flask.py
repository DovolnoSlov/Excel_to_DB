from sqlalchemy import create_engine, MetaData, Table, Integer, Column, Date, Float
import os
import shutil
import time
import pandas as pd
import yaml
import logging

logging.basicConfig(level=logging.INFO, filename="py_log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

__config_path = os.path.abspath(os.path.join('.', 'config.yaml'))
with open(os.path.join(__config_path)) as f_config:
    __config = yaml.safe_load(f_config)

PATH_TO_DATA = os.path.abspath(os.path.join('.', *__config['data']['path']))
TYPE_OF_DATABASE = __config['database']['type']
PATH_TO_DATABASE = os.path.abspath(os.path.join(__config['database']['path'], __config['database']['name']))


class AppNoFlask:
    """
    Поиск файлов с расширением xlsx, и выгрузка данных из найденных файлов в БД

    Args:
        path_data (str): const: путь до каталога-хранилища файлов
        type_database (str): const: тип базы данных
        path_database (str): const: путь до базы данных
    """

    def __init__(self,
                 path_data: str = PATH_TO_DATA,
                 type_database: str = TYPE_OF_DATABASE,
                 path_database: str = PATH_TO_DATABASE):

        self.path_data = path_data
        self.type_database = type_database
        self.path_database = path_database

    def run(self) -> None:
        """ Запуск поиска файлов """

        self.__find_files_xlsx_and_to_sql()

    def __find_files_xlsx_and_to_sql(self) -> None:
        """ Поиск файлов с расширением xlsx """

        for filename in os.listdir(self.path_data):
            if filename.endswith('.xlsx'):
                self.__data_to_sql(filename)

    def __data_to_sql(self, file_name: str) -> None:
        """ Добавление данных из файла в БД, с последующим удалением файла """

        path_to_file = os.path.join(self.path_data, file_name)

        try:
            df_data = self.__get_df(path_to_file)
            logging.info(f"Successful download from file: {file_name}")

        except Exception as E:
            file_move_path = os.path.join(self.path_data, 'problem_files', file_name)
            shutil.move(path_to_file, file_move_path)
            logging.error(f"Wrong file: {file_name}\n{E}", exc_info=True)

        else:
            self.__uploading_to_db(path_to_file, df_data)

    def __get_df(self, path_to_file: str) -> pd.DataFrame:
        """
        Выгрузка данных из файла в датафрейм.
        Дата парсится из разных форматов.
        """
        df = pd.read_excel(path_to_file, parse_dates=['Rep_dt'], decimal=',')
        df['Rep_dt'] = pd.to_datetime(df['Rep_dt'], format='mixed', dayfirst=False)
        df['Delta'] = df['Delta'].astype(float)
        return df

    def __uploading_to_db(self, path_to_file: str, data: pd.DataFrame) -> None:
        """ Выгрузка данных из датафрейма в базу данных через транзакцию. """

        engine = create_engine(f"{self.type_database}:///{self.path_database}")
        with engine.connect() as connect:
            transaction = connect.begin()
            try:
                data.to_sql(name='deltatable', con=connect, if_exists='append',
                            index=False, dtype={"Rep_dt": Date, "Delta": Float})
                transaction.commit()
                os.remove(path_to_file)
                logging.info(f"Successful transaction to DB. Original file deleted")
            except Exception as E:
                logging.error(f"Failed transaction!\n{E}", exc_info=True)
                transaction.rollback()


def __create_db(path_database: str = PATH_TO_DATABASE) -> None:
    """ Создание БД """

    metadata = MetaData()
    engine = create_engine(f'sqlite:///{path_database}')
    deltatable = Table('deltatable', metadata,
        Column('id', Integer, primary_key=True),
        Column('Rep_dt', Date),
        Column('Delta', Float),
    )
    metadata.create_all(engine)
    logging.info(f"Create DB")


def main() -> None:
    """ Функция для добавления данных в БД из файлов с расширением xlsx, с поиском раз в минуту """

    app = AppNoFlask()
    while True:
        app.run()
        time.sleep(60)


if __name__ == "__main__":
    if not os.path.exists(PATH_TO_DATABASE):
        __create_db()

    main()
