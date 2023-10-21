from sqlalchemy import create_engine, MetaData, Table, Integer, Column, Date, Float
import pandas as pd
import os
import yaml

__config_path = os.path.abspath(os.path.join('.', 'config.yaml'))
with open(os.path.join(__config_path)) as f_config:
    config = yaml.safe_load(f_config)

TYPE_OF_DATABASE = config['database']['type']
PATH_TO_DATABASE = os.path.abspath(os.path.join(config['database']['path'], config['database']['name']))


def get_data_from_db(type_database: str = TYPE_OF_DATABASE,
                     path_database: str = PATH_TO_DATABASE) -> pd.DataFrame:
    """
    Запрос данных из БД
    с формированием в представлении отдельного поля DeltaLag
    с данными из поля Delta со смещением на 2 месяца назад.
    С финальной сортировкой по дате для наглядности смещения.
    Предполагается, что данные по конкретному месяцу представлены однократно.
    Последние 2 месяца без значений в DeltaLag сохранены.
    """

    engine = create_engine(f'{type_database}:///{path_database}')
    with engine.connect() as connect:

        select = ('SELECT Rep_dt, Delta, '
                  'LEAD(Delta, 2) OVER (ORDER BY Rep_dt) as DeltaLag '
                  'FROM deltatable '
                  'ORDER BY Rep_dt')

        df_database = pd.read_sql(select, connect, parse_dates=['Rep_dt'])
    return df_database


def get_data_from_db_pandas(type_database: str = TYPE_OF_DATABASE,
                            path_database: str = PATH_TO_DATABASE) -> pd.DataFrame:
    """
    Запрос данных из БД
    с реализацией в pandas отдельного поля DeltaLag
    с данными из поля Delta со смещением на 2 месяца назад.
    С финальной сортировкой по дате для наглядности смещения.
    Предполагается, что данные по конкретному месяцу представлены однократно.
    Последние 2 месяца без значений в DeltaLag сохранены.
    """

    engine = create_engine(f'{type_database}:///{path_database}')
    with engine.connect() as connect:

        select = ('SELECT Rep_dt, Delta '
                  'FROM deltatable')

        df_pandas = pd.read_sql(select, connect, parse_dates=['Rep_dt'])
        df_pandas['DeltaLag'] = df_pandas.sort_values('Rep_dt')['Delta'].shift(-2)
        df_pandas.sort_values('Rep_dt', ascending=True, inplace=True)
    return df_pandas


if __name__ == "__main__":
    get_df_database = get_data_from_db()
    print(get_df_database)

    get_df_pandas = get_data_from_db_pandas()
    print('\n', get_df_pandas)
