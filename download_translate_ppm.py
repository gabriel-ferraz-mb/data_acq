# -*- coding: utf-8 -*-
"""
Created on Wed Sep 20 11:21:15 2023

@author: gabriel.ferraz


downlaod_translate_ppm.py

This script is used to:
    1. retrieves data form PPM/IBGE Herd
    2. retrieves data form PPM/IBGE Product
    3. upload the result into Postgres database
 
@author: gabriel.ferraz

Created on Fry Sep 25 18:00:00 2023

"""
# =============================================================================
# Import Libs
# =============================================================================
import pandas as pd
from urllib.request import Request, urlopen
import json
import requests
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy import inspect
from sqlalchemy import select
pd.options.mode.chained_assignment = None
from joblib import Parallel, delayed
# =============================================================================
# Define translation dictionary
# =============================================================================
eng_to_pt = {'Tabela 74 - Produção de origem animal, por tipo de produto':'Production of animal origin, by type of product',
             'Cód.':'Code',
             'Brasil e Município':'Municipality',
             'Ano':'Year',
             'Tipo de produto de origem animal':'Type of product of animal origin',
             'Variável':'Variable',
             'Leite':'Milk',
             'Ovos de galinha':'Chicken eggs',
             'Ovos de codorna':'Quail eggs',
             'Mel de abelha': "Bee's honey",
             'Casulos do bicho-da-seda':'Silkworm cocoons',
             'Lã':'Wool',
             'Produção de origem animal (Mil litros)':'Production of animal origin (Thousand liters)',
             'Produção de origem animal (Mil dúzias)':'Production of animal origin (Thousand dozens)',
             'Produção de origem animal (Quilogramas)':'Production of animal origin (Kilograms)',
             'Valor da produção (Mil Reais)':'Production value (Thousand Reais)',
             'Tabela 3939 - Efetivo dos rebanhos, por tipo de rebanho':'Herd numbers, by type of herd',
             'Tipo de rebanho': 'Herd type',
             'Bovino':'Bovine',
             'Bubalino':'Buffalo',
             'Equino':'Equine',
             'Efetivo dos rebanhos (Cabeças)':'Number of individuals',
             'Suíno - total':'Pig (total)',
             'Suíno - matrizes de suínos':'Pig (matrix)',
             'Caprino':'Goat',
             'Ovino':'Sheep',
             'Galináceos - total':'Gallinaceous (total)',
             'Galináceos - galinhas':'Gallinaceous - chickens',
             'Codornas':'Quails',
             'Vacas ordenhadas (Cabeças)':'Milk cows (herd size)',
             'Ovinos tosquiados nos estabelecimentos agropecuários (Cabeças)':'Shorned sheeps (herd size)',
             'Número de estabelecimentos agropecuários que produziram leite de vaca (Unidades)':'Number of farms with milk production',
             'Vacas ordenhadas nos estabelecimentos agropecuários (Cabeças)':'Milk cows (herd size)',
             'Quantidade produzida de leite de vaca (Mil litros)':'Milk production (Thousand liters)',
             'Bovinos':'Bovine',
             'Bubalinos':'Buffalo',
             'Equinos':'Equine',
             'Asininos':'Donkeys',
             'Muares':'Mules',
             'Caprinos':'Goat',
             'Número de cabeças (Cabeças)':'Number of individuals',
             'Ovinos':'Sheep',
             'Suínos':'Pig (total)',
             'Galinhas, galos, frangas, frangos e pintos':'Gallinaceous (total)',
             'Codornas':'Quails',
             'Patos, gansos, marrecos, perdizes e faisões':'Ducks (total)',
             'Perus':'Turkeys',
             'Avestruzes':'Ostriches',
             'Coelhos':'Rabbits',
             'Unnamed: 5':'Value',
              'Valor':'Value',
              '-':0,
              '...':0,
              '..':0,
              'X':0}

# from os import listdir
# from os.path import isfile, join
# p = r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\SUSTENTABILIDADE\To_Translate'
# # onlyfiles = [f for f in listdir(p) if isfile(join(p, f))]


# # # df1.ffill(inplace = True)
# # # df2  = pd.read_excel(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\SUSTENTABILIDADE\db_ppm_herd_size_2021.xlsx')
# # # df2.ffill(inplace = True)
# onlyfiles = ['db_ppm_animal_production_2000_2021.csv']
# # 'db_ppm_shorn_sheep.csv']
# for x in onlyfiles:
#     new_f = p+ '\\'+x
#     if new_f.endswith('xlsx'):
#         data = pd.read_excel(new_f)
#         new_header = data.iloc[0] #grab the first row for the header
#         data = data[1:] #take the data less the header row
#         data.columns = new_header
#         new_f = new_f.replace('.xlsx', '_EN.xlsx')
#     else:
#         data = pd.read_csv(new_f, sep = ';').reset_index()
#         new_header = data.iloc[0] #grab the first row for the header
#         data = data[1:] #take the data less the header row
#         data.columns = new_header
#         new_f = new_f.replace('.csv', '_EN.csv')
#     for old, new in eng_to_pt.items():
#         data= data.replace(old, new, regex = False)
#         data.columns = data.columns.str.replace(old, new)
#     if new_f.endswith('xlsx'):
#         data.to_excel(new_f, index = False)
#     else:
#         data.to_csv(new_f, index = False, sep = ';')
# df1.to_excel(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\SUSTENTABILIDADE\db_ppm_animal_production_2021_EN.xlsx', index=False)
# df2.to_excel(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\SUSTENTABILIDADE\db_ppm_herd_size_2021_EN.xlsx', index=False)


d  = pd.read_excel(r'C:\Users\gabriel.ferraz\OneDrive - Kynetec\CIENCIA DE DADOS\SUSTENTABILIDADE\db_ppm_animal_production_2021_EN.xlsx',
                      sheet_name='data')
d = d[['Type of product of animal origin', 'Variable']]
d = d[d.Variable != 'Production value (Thousand Reais)'].drop_duplicates()

type_of_product_dict = dict(zip(d['Type of product of animal origin'], d.Variable))
'''dict(zip(df.A,df.B))'''
# =============================================================================
# Create engine to conect w/ Postgres
# =============================================================================
usuario = 'gabriel'
senha = '6dnbhJsL0V'
host = 'apitransfer.sparkbipdata.com'
porta= '5432'
banco = 'postgres'
global engine
dbschema='public' 
engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}',
                       connect_args={'options': '-csearch_path={}'.format(dbschema)})

# with engine.connect() as con:

#    con.execute('DROP TABLE public.sur_ibge_ppm_herd')
#    con.execute('DROP TABLE public.sur_ibge_ppm_product')
#    con.commit()
#    con.close()
# =============================================================================
# Crate tables if do not exit
# =============================================================================
if not inspect(engine).has_table('sur_ibge_ppm_herd'):
    metadata = MetaData()
    ppm_her = Table('sur_ibge_ppm_herd', metadata,
      Column('code', String),
      Column('year', String),
      Column('type_of_herd', String),
      Column('value', Integer)
    )
    metadata.create_all(engine)

if not inspect(engine).has_table('sur_ibge_ppm_product'):
    metadata = MetaData()
    ppm_her = Table('sur_ibge_ppm_product', metadata,
      Column('code', String),
      Column('year', String),
      Column('type_of_product', String),
      Column('unit', String),
      Column('value', Integer)
    )
    metadata.create_all(engine)

if not inspect(engine).has_table('sur_ibge_ppm_value'):
    metadata = MetaData()
    ppm_her = Table('sur_ibge_ppm_value', metadata,
      Column('code', String),
      Column('year', String),
      Column('type_of_product', String),
      Column('unit', String),
      Column('value', Integer)
    )
    metadata.create_all(engine)
    
# =============================================================================
# Crate Year and Cd_mun list to iterate
# =============================================================================
geo_mun = pd.read_sql_query("Select * from homolog.geo_municipalities_full", con = engine)
cd_mun = list(set(geo_mun['cd_mun']))
#yl = list(range(2000, 2022))
yl = [2021]
arg_pairs = [(i,j) for i in yl for j in cd_mun]

# =============================================================================
# download Herd data for each year and mun and put it into pandas df
# =============================================================================
precondition = pd.read_sql_query('SELECT code , year FROM public.sur_ibge_ppm_herd', con= engine)
precondition_l = [(i,j) for i in pd.to_numeric(precondition.year).to_list() for j in precondition.code.to_list()]

remain = [x for x in arg_pairs if x not in precondition_l]

results = []
count = 0
metadata = MetaData()
table= Table('sur_ibge_ppm_herd', metadata,autoload=True, autoload_with=engine)

for arg in remain:
    year,i = arg
    
    url = """https://servicodados.ibge.gov.br/api/v3/agregados/3939/periodos/{0}/variaveis/105?localidades=N1[all]|N6[{1}]&classificacao=79[all]""".format(year, i)
    j = None
    t = 0
    
    while t < 10 and j is None:
        try:
            request=Request(url)
            response = urlopen(request)
            data =  response.read()
            j = json.loads(data)
            dado = []
            for a in j[0]['resultados']:
                animal = list(a['classificacoes'][0]['categoria'].values())[0]
                valor = list(a['series'][1]['serie'].values())[0]
                linha = {'code': i, 'year':year, 'type_of_herd':animal, 'value': valor}
                for key, value in linha.items():
                   if value in eng_to_pt:
                       linha[key] = eng_to_pt[value]
                dado.append(linha)
            with engine.connect() as conn:
                conn.execute(table.insert(),dado)
                    #conn.commit()
                    #conn.close()
                #results.append(linha)
        except:
            t+=1
            pass
    
    if j is None:
        print(f'Comb {year} {i} had an error')
        continue
    count += 1
    p = round((count/len(arg_pairs))*100,5)
    print("""Municipality {0} Year {1} done. Herd {2}% completed""".format(i, year, p))
    


# =============================================================================
# Apply translation
# =============================================================================
# df1 = pd.DataFrame(results)
# for old, new in eng_to_pt.items():
#     df1 = df1.replace(old, new, regex=False)
#     df1.columns = df1.columns.str.replace(old, new)
    #df2 = df2.replace(old, new, regex=False)
    #df2.columns = df2.columns.str.replace(old, new)
# # =============================================================================
# # Charge it to Postgres
# # =============================================================================
# try:
#     #engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}')
#     if engine:
#         df1.to_sql("sur_ibge_ppm_herd", con=engine, schema="public", if_exists="replace", index = False)
#         #df2.to_sql("sur_ibge_ppm_product", con=engine, schema="public", if_exists="replace", index = False)
#         print("\n>>> ibge_ppm - atualizado")
#         #del(engine)
# except Exception as e:
#     print(e)
#     print("Falha na conexão com servidor \n")


# =============================================================================
# download Product data for each year and mun and put it into pandas df
# =============================================================================

#yl.append(2022)
yl = [2022]
arg_pairs = [(i,j) for i in yl for j in cd_mun]
precondition = pd.read_sql_query('SELECT code , year FROM public.sur_ibge_ppm_product', con= engine)
precondition_l = [(i,j) for i in pd.to_numeric(precondition.year).to_list() for j in precondition.code.to_list()]

remain = [x for x in arg_pairs if x not in precondition_l]

results = []
count = 0
metadata = MetaData()
table= Table('sur_ibge_ppm_product', metadata,autoload=True, autoload_with=engine)

for arg in remain:
    year,i = arg
    url = """https://servicodados.ibge.gov.br/api/v3/agregados/74/periodos/{0}/variaveis/106%7C215?localidades=N6[{1}]&classificacao=80[2682,2685,2686,2687,2683,2684]""".format(year, i)
    
    j = None
    t = 0
    
    while t < 10 and j is None:
        try:
            request=Request(url)
            response = urlopen(request)
            data =  response.read()
            j = json.loads(data)
            dado = []
            for a in j[0]['resultados']:
                produto = list(a['classificacoes'][0]['categoria'].values())[0]
                valor = list(a['series'][0]['serie'].values())[0]
                linha = {'code': i, 'year':year, 'type_of_product':produto, 'value': valor}
                for key, value in linha.items():
                   if value in eng_to_pt:
                       linha[key] = eng_to_pt[value]
                linha['unit'] = type_of_product_dict[linha['type_of_product']]
                dado.append(linha)
            with engine.connect() as conn:
                conn.execute(table.insert(),dado)
        except:
            t+=1
            pass
    
    if j is None:
        print(f'Comb {year} {i} had an error')
        
        #continue
    count += 1
    p = round((count/len(arg_pairs))*100,5)
    print("""Municipality {0} Year {1} done. Product {2}% completed""".format(i, year, p))
    
# =============================================================================
# download Product Value data for each year and mun and put it into pandas df
# =============================================================================

#yl.append(2022)
yl = [2022]
arg_pairs = [(i,j) for i in yl for j in cd_mun]
precondition = pd.read_sql_query('SELECT code , year FROM public.sur_ibge_ppm_value', con= engine)
precondition_l = [(i,j) for i in pd.to_numeric(precondition.year).to_list() for j in precondition.code.to_list()]

remain = [x for x in arg_pairs if x not in precondition_l]

results = []
count = 0
metadata = MetaData()
table= Table('sur_ibge_ppm_value', metadata,autoload=True, autoload_with=engine)

for arg in remain:
    year,i = arg
    url = """https://servicodados.ibge.gov.br/api/v3/agregados/74/periodos/{0}/variaveis/215?localidades=N6[{1}]&classificacao=80[all]""".format(year, i)
    
    j = None
    t = 0
    
    while t < 10 and j is None:
        try:
            request=Request(url)
            response = urlopen(request)
            data =  response.read()
            j = json.loads(data)
            dado = []
            for a in j[0]['resultados']:
                produto = list(a['classificacoes'][0]['categoria'].values())[0]
                valor = list(a['series'][0]['serie'].values())[0]
                linha = {'code': i, 'year':year, 'type_of_product':produto, 'value': valor}
                for key, value in linha.items():
                   if value in eng_to_pt:
                       linha[key] = eng_to_pt[value]
                linha['unit'] = 'Thousand Reais'
                dado.append(linha)
            with engine.connect() as conn:
                conn.execute(table.insert(),dado)
        except:
            t+=1
            pass
    
    if j is None:
        print(f'Comb {year} {i} had an error')
        
        #continue
    count += 1
    p = round((count/len(arg_pairs))*100,5)
    print("""Municipality {0} Year {1} done. Value {2}% completed""".format(i, year, p))

# =============================================================================
# Apply translation
# =============================================================================
# df2 = pd.DataFrame(results)
# for old, new in eng_to_pt.items():
#     df2 = df2.replace(old, new, regex=False)
#     df2.columns = df2.columns.str.replace(old, new)
#     #df2 = df2.replace(old, new, regex=False)
#     #df2.columns = df2.columns.str.replace(old, new)
# # # =============================================================================
# # # Charge it to Postgres
# # # =============================================================================
# try:
#     #engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}')
#     if engine:
#         df2.to_sql("sur_ibge_ppm_product", con=engine, schema="public", if_exists="replace", index = False)
#         #df2.to_sql("sur_ibge_ppm_product", con=engine, schema="public", if_exists="replace", index = False)
#         print("\n>>> ibge_ppm - atualizado")
#         #del(engine)
# except Exception as e:
#     print(e)
#     print("Falha na conexão com servidor \n")

# # for old, new in eng_to_pt.items():
# #     df1 = df1.replace(old, new, regex=False)
# #     df1.columns = df1.columns.str.replace(old, new)
# #     df2 = df2.replace(old, new, regex=False)
# #     df2.columns = df2.columns.str.replace(old, new)
# # # =============================================================================
# # # Charge it to Postgres
# # # =============================================================================
# # try:
# #     #engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}')
# #     if engine:
# #         df1.to_sql("sur_ibge_ppm_herd", con=engine, schema="public", if_exists="replace", index = False)
# #         df2.to_sql("sur_ibge_ppm_product", con=engine, schema="public", if_exists="replace", index = False)
# #         print("\n>>> ibge_ppm - atualizado")
# #         #del(engine)
# # except Exception as e:
# #     print(e)
# #     print("Falha na conexão com servidor \n")

