import os
import time
import logging
import pandas as pd
import requests
import json
import sidrapy
from argparse import ArgumentParser
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from alive_progress import alive_bar

def main() -> None:
    logging.info('Initializing process...')

    if args.create:
        create_db()

    mun_list = get_mun_list()
    sidra_catalog = get_sidra_catalog()

    if args.table:
        sidra_catalog = {table:params for table,params in sidra_catalog.items() if table in args.table }

    if args.debug:
        mun_list = [3538709]

    for table_name in sidra_catalog:

        if args.reset:
            reset_table(table_name)

        print(f'{table_name}:')
        logging.info(f'Table name: {table_name}')

        with alive_bar(len(mun_list)) as bar:
            for cd_mun in mun_list:
                
                data = extract_data(cd_mun, sidra_catalog[table_name])
                
                if not data.empty:
                    data = transform_data(data) 
                    load_data(data, table_name=f'rawdata_{table_name}')

                    #TODO Utilizar .pipe(), por algum motivo não deu certo...
                    # (
                    #     transform_data(data).pipe(load_data)
                    # )

                else:
                    logging.warning('Extracted data is empty.')
                    continue
                bar()

    logging.info('Process Complete!')

def get_mun_list() -> list:
    try:
        df = pd.DataFrame(json.loads(requests.get(IBGE_MUNS_URL).text))
        mun_list = df['id'].tolist()
        if len(mun_list) > 0:
            return mun_list
        
    except Exception as e:
        logging.error(f'get_mun_list: {e}')

def get_sidra_catalog() -> dict:
    try:
        with open(CATALOG_FILE) as f:
            catalog = json.load(f)

        return catalog
 
    except Exception as e:
        logging.error(f'get_sidra_catalog: {e}')

def extract_data(cd_mun:int, params:dict) -> pd.DataFrame:
    tries = 1
    logging.info(f'{cd_mun}: Downloading...')
    while tries <= 5:
        try: 
            df = sidrapy.get_table(
                **params,
                territorial_level="6",
                ibge_territorial_code=cd_mun,
                period=args.date,
                header='n'
            )

            logging.info(f'{cd_mun}: Download complete!')
            return df

        except Exception as e:
            logging.warning(f'{cd_mun}: {e}')
            tries += 1
            time.sleep(60)

def transform_data(df:pd.DataFrame) -> pd.DataFrame:

    column_names = {
        'NC': 'cd_nv_territory',
        'NN': 'nv_territory' ,
        'MC': 'cd_unit',
        'MN': 'unit',
        'V': 'value',
        'D1C': 'cod_ibge',
        'D1N': 'nm_ibge',
        'D2C': 'cd_year',
        'D2N': 'year',
        'D3C': 'cd_variable',
        'D3N': 'nm_variable',
        'D4C': 'cd_category',
        'D4N': 'nm_category',
        'D5C': 'cd_landuse',
        'D5N': 'nm_landuse',
        'D6C': 'cd_activity',
        'D6N': 'nm_activity',
        'D7C': 'cd_gender',
        'D7N': 'nm_gender'
    }

    try:
        df = df.rename(columns=column_names)
        # df['value'] = ['0' if v in ('-','X','..','...') else v for v in df.value]
        return df
    
    except Exception as e:
        logging.error(f'transform_data: {e}')

def load_data(df:pd.DataFrame, table_name:str) -> None:
    try:
        logging.info(f'Loading to Database...')
        with ENGINE.connect() as conn:
            df.to_sql(table_name, conn, schema='rawdata', index=False, if_exists='append')

        logging.info(f'Load complete!')

    except Exception as e:
        logging.error(f'load_data: {e}')

def reset_table(table_name:str) -> None:
    try:
        logging.info(f'Reseting table rawdata_{table_name}...')
        with ENGINE.connect() as conn:
            conn.execute(text(f'TRUNCATE rawdata.rawdata_{table_name} RESTART IDENTITY;'))
            conn.commit()
    
    except Exception as e:
        logging.error(f'{table_name}: {e}')

def create_db() -> None:
    try:
        logging.info(f'Creating rawdata schema and tables in {DATABASE}')
        with ENGINE.connect() as conn:
            with open(os.path.join(SQL_PATH,'rawdata_schema.sql'), encoding="utf-8") as file:
                query = text(file.read())
            conn.execute(query)
            conn.commit()
    except Exception as e:
        logging.error(f'create_db: {e}')
        raise

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-d', '--date', type=str, required=True, help='Dates do be downloaded.')
    parser.add_argument('-t', '--table', type=str, nargs='+', help='List tables to be downlaoded.')
    parser.add_argument('-c', '--create', action='store_true', help='Create rawdata schema and tables in database.')
    parser.add_argument('-r', '--reset', action='store_true', help='Reset results for all tables if --table is not specifyied.') 
    parser.add_argument('--debug', action='store_true', help='Set mun_list for one municipality')
    args = parser.parse_args()
   
    load_dotenv(find_dotenv())

    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')
    DATABASE = os.getenv('DATABASE')
    ENGINE = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}', poolclass=NullPool)
    
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    SQL_PATH = os.path.join(ROOT_PATH,'sql')
    LOG_FILE = os.path.join(ROOT_PATH,'sidra_downloader.log')
    CATALOG_FILE = os.path.join(ROOT_PATH,'sidra_catalog.json')

    IBGE_MUNS_URL = 'https://servicodados.ibge.gov.br/api/v1/localidades/municipios'

    logging.basicConfig(
        filename=LOG_FILE,
        filemode='a',
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    main()