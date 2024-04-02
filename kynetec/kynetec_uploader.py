import os
import json
import logging
import pandas as pd
from argparse import ArgumentParser
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

def main() -> None:
    logging.info('Initializing process...')

    if args.create:
        create_db()

    config = get_config_files()
    source = config[args.source]

    print('Process Running...')
    csv = get_csv(args.file, args.sep, args.encoding)
    table = get_table(source["table_name"])

    if check_data_structure(csv,table):
        mode = 'replace' if args.reset else 'append'
        if args.reset:
            reset_table(source["table_name"])
        
        load_data(csv, source["table_name"], mode)
        print('Process Complete. See the logfile for more details.')

    logging.info('Process Complete!')
    
def get_config_files() -> dict:
    try:
        with open(CONFIG_FILE) as f:
            config = json.load(f)

        return config
 
    except Exception as e:
        logging.error(f'get_config_files: {e}')

def get_csv(file, sep, encoding) -> pd.DataFrame:
    try:
        logging.info(f'Loading CSV: {file}')
        df = pd.read_csv(file, sep=sep, encoding=encoding, dtype=object)
        
        return df
    except Exception as e:
        logging.error(f'get_csv: {e}')

def get_table(table_name:str) -> pd.DataFrame:
    logging.info(f'Fetching from database: rawdata.{table_name}')
    try:
        query = f'SELECT * FROM rawdata.{table_name} LIMIT 1'
        df = pd.read_sql(query, ENGINE)

        return df
    
    except Exception as e:
        logging.error(f'get_table: {e}')

def check_data_structure(csv:pd.DataFrame, table:pd.DataFrame) -> bool:
    df1_struct = csv.dtypes
    df2_struct = table.dtypes

    check = df1_struct.eq(df2_struct).all() 

    if not check:
        logging.info('CSV and Raw data Table structures do not match')
        logging.warning(f'CSV Columns: {csv.columns}' )
        logging.warning(f'Raw data Table Columns: {table.columns}')

        raise Exception('check_data_structure: Data structure do not match.')

    logging.info('Data Check: ok')
    return check

def reset_table(table_name:str) -> None:
    try:
        logging.info(f'Reseting table: rawdata.{table_name}')
        with ENGINE.connect() as conn:
            conn.execute(text(f'TRUNCATE rawdata.{table_name} RESTART IDENTITY;'))
            conn.commit()
    
    except Exception as e:
        logging.error(f'{table_name}: {e}')        

def load_data(df:pd.DataFrame, table_name:str, mode:str) -> None:
    try:
        logging.info(f'Loading data into database: rawdata.{table_name}')
        with ENGINE.connect() as conn:
            df.to_sql(table_name, conn, schema='rawdata', index=False, if_exists=mode)
    except Exception as e:
        logging.error(f'upload_data: {e}')

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

def check_args() -> None:
    config = get_config_files()
    config_keys = list(config.keys())
    
    for key, value in vars(args).items():
        if (key == 'file') and (('.' not in value) or (value.split('.')[1] not in ['csv','CSV'])):
            raise Exception(f'Parameter {key} incorrect. Expected file with .csv extension.')
        
        if (key == 'source') and (value not in config_keys):
            raise Exception(f'Parameter {key} incorrect. Expected one of {config_keys}')

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-f','--file', type=str, required=True, help='CSV to be uploaded.')
    parser.add_argument('-s','--source',type=str, required=True, help='Table to be updated.')
    parser.add_argument('-c', '--create', action='store_true', help='Create rawdata schema and tables in database.')
    parser.add_argument('-r', '--reset', action='store_true', help='Reset table before upload.') 
    parser.add_argument('--sep', type=str, nargs='?', const=';', default=';', help='CSV separator.')
    parser.add_argument('--encoding', type=str, nargs='?', const='latin1', default='latin1', help='CSV encoding.')
    args = parser.parse_args()
    
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    SQL_PATH = os.path.join(ROOT_PATH,'sql')
    LOG_FILE = os.path.join(ROOT_PATH,'kynetec_uploader.log')
    CONFIG_FILE = os.path.join(ROOT_PATH,'config_files.json')
    
    check_args()
    load_dotenv(find_dotenv())

    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')
    DATABASE = os.getenv('DATABASE')
    ENGINE = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')    

    logging.basicConfig(
        filename=LOG_FILE,
        filemode='a',
        format='%(asctime)s\t%(levelname)s\t%(message)s',
        level=logging.INFO
    )
    
    main()