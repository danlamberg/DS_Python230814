# ETL Microdados do INEP = Educação Superior
# Autor: Daniel Lamberg 
# Data: 14/08/2023

#Conectar na base do DW_INEP
import mysql.connector

#Importando a biblioteca Pandas
import pandas as pd

#Dicionário com a config fo banco para conexão
config = {
    'user': 'root',
    'password':'minhasenha',
    'host':'localhost',
    'database':'dw_inep',
    'port':'3306'
}
try:
    conn = mysql.connector.connect(**config)
    dados = pd.read_csv('.CSV',
                        sep=';',
                        encoding='iso-8859-1')
    print(dados.head())
    print(dados.count())
    print(len(dados))
    print(dados.columns)
    print(dados.info())
    print(dados.dtypes)

    dados_uf = dados['NO_UF'].unique()
    dados_uf = pd.DataFrame(dados['NO_UF'].unique(), columns = ['uf'])
    cursor = conn.cursor()

    for i, r in dados_uf.iterrows():
        # print(i,r['uf'])
        insert_statement = 'insert into dim_uf (tf_uf, uf) values (' \
              + str(i) +',\'' \
              + str(r['uf']) +'\')'
        # print(insert_statement)
        cursor.execute(insert_statement)
        conn.commit()
    
    # print(dados_uf[5])
    # print(type(dados_uf))
    # print(dados_uf)
    # for i in range(0, len(dados_uf)):
    #     print(dados_uf[i])
    # print(dados['NO_UF'])
except Exception as e:
    print(e)
