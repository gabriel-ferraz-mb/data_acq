import os
import time
import logging
import pandas as pd
import json
from argparse import ArgumentParser
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

def main() -> None:
    logging.info('Initializing process...')

    catalog = get_catalog()

    if args.table:
        catalog = {table:params for table,params in catalog.items() if table in args.table}
    
    print('Process Running...')
    for table_name in catalog:
        
        if args.reset:
            reset_table(table_name)
        
        data = extract_data(table_name, catalog[table_name])

        if not data.empty:
            load_data(data, table_name=f'rawdata_{table_name}')

        else:
            logging.warning('Extracted data is empty.')
            continue        

    print('Process Complete!')
    logging.info('Process Complete!')

def get_catalog() -> dict:
    try:
        with open(CATALOG_FILE) as f:
            catalog = json.load(f)
        return catalog
 
    except Exception as e:
        logging.error(f'get_catalog: {e}')

def extract_data(table_name:str, param:str) -> pd.DataFrame:
    logging.info(f'{table_name}: Downloading...')
    try:
        url = CONAB_BASE_URL + param + '.txt'
        return pd.read_csv(url, sep=";",encoding = "UTF-8")   
    except Exception as e:
        logging.error(f'extract_data: {e}')
    
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

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-r', '--reset', action='store_true', help='Reset results for all tables if --table is not specifyied.') 
    parser.add_argument('-t', '--table', type=str, nargs='+', help='List tables to be downlaoded.')
    args = parser.parse_args()
   
    load_dotenv(find_dotenv())

    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')
    DATABASE = os.getenv('DATABASE')
    ENGINE = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}', poolclass=NullPool)
    
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    LOG_FILE = os.path.join(ROOT_PATH,'conab_downloader.log')
    CATALOG_FILE = os.path.join(ROOT_PATH,'conab_catalog.json')

    CONAB_BASE_URL="https://portaldeinformacoes.conab.gov.br/downloads/arquivos/"

    logging.basicConfig(
        filename=LOG_FILE,
        filemode='a',
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )

    main()