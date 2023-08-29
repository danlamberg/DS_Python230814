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
    cursor = conn.cursor()

    dados = pd.read_csv('E:/BSI/DS_Aula/Microdados do Censo da Educação Superior 2020/dados/MICRODADOS_CADASTRO_CURSOS_2020.CSV',
                        sep=';',
                        encoding='iso-8859-1')
    dados_municipio = dados.fillna('')
    # print(dados.head())
    # print(dados.count())
    # print(len(dados))
    # print(dados.columns)
    # print(dados.info())
    # print(dados.dtypes)

    # dados_uf = dados['NO_UF'].unique()
    # dados_uf = pd.DataFrame(dados['NO_UF'].unique(), columns = ['uf'])
    # cursor = conn.cursor()

    # for i, r in dados_uf.iterrows():
    #     # print(i,r['uf'])
    #     insert_statement = 'insert into dim_uf (tf_uf, uf) values (' \
    #           + str(i) +',\'' \
    #           + str(r['uf']) +'\')'
    #     # print(insert_statement)
    #     cursor.execute(insert_statement)
    #     conn.commit()
    
    # print(dados_uf[5])
    # print(type(dados_uf))
    # print(dados_uf)
    # for i in range(0, len(dados_uf)):
    #     print(dados_uf[i])
    # print(dados['NO_UF'])
    #Municipio
    # dados_municipio = pd.DataFrame(dados['NO_MUNICIPIO'].unique(), columns = ['Municipio'])
    # dados_municipio['Municipio'].replace('\' ','' )
    # dados_municipio = dados_municipio.fillna('')
    # for i, r in dados_municipio.iterrows():        
    #     municipio = r['Municipio']
    #     municipio = municipio.replace("'", "")
    #     insert_statement = f"insert into dim_municipio (tf_municipio, municipio) values({i}, '{municipio}')"
    #     print(insert_statement)
    #     cursor.execute(insert_statement)
    #     conn.commit()

    #Fact_Matriculas
    # for i, r in dados.iterrows():
    #     matriculas = r['QT_INSCRITO_TOTAL']
    #     municipio = r['NO_MUNICIPIO']
    #     municipio = municipio.replace("'", "")
    #     uf = r['NO_UF']
    #     uf = uf.replace("'", "")
    #     insert_statement = f"insert into fact_matriculas (matriculas, tf_munipio, tf_uf)"\
    #                        "select * from "\
    #                        "(select {matriculas} as matriculas) as matriculas, "\
    #                        "(select distinct tf_municipio from dim_municipio where municipio = '{municipio}' limit 1) as tf_municipio, "\
    #                        "(select distinct tf_uf from dim_uf where uf = '{uf}' limit 1) as tf_uf"
    #     cursor.execute(insert_statement)
    #     conn.commit()

    #modalidade ensino MEU
#     dados_modalidade = pd.DataFrame(dados['TP_MODALIDADE_ENSINO'].unique(), columns = ['tp_modalidade_ensino'])
#     for i, r in dados_modalidade.iterrows():
#         if r['tp_modalidade_ensino'] == 1:
#             insert_statement = f"insert into dim_modalidade (tf_modalidade, modalidade) values({r['tp_modalidade_ensino']}, 'Presencial')"
#         elif r['tp_modalidade_ensino'] == 2:
#             insert_statement = f"insert into dim_modalidade (tf_modalidade, modalidade) values({r['tp_modalidade_ensino']}, 'EAD')"

#             cursor.execute(insert_statement)
#             conn.commit()
# except Exception as e:
#     print(e)


# Modalidade ensino DIOGO
#     dados_modalidade = pd.DataFrame(dados['TP_MODALIDADE_ENSINO'].unique(), columns = ['tp_modalidade_ensino'])
#     for i, r in dados_modalidade.iterrows():
#         if r['tp_modalidade_ensino'] == 1:
#             insert_statement = f"insert into dim_modalidade (tf_modalidade, modalidade) values({r['tp_modalidade_ensino']}, 'Presencial')"
#         elif r['tp_modalidade_ensino'] == 2:
#             insert_statement = f"insert into dim_modalidade (tf_modalidade, modalidade) values({r['tp_modalidade_ensino']}, 'EAD')"

#         cursor.execute(insert_statement)
#         conn.commit()
# except Exception as e:
#     print(e)

# CURSO
#     dados_curso = pd.DataFrame(dados['NO_CURSO'].unique(), columns= ['curso'])
#     for i,r in dados_curso.iterrows():
#         insert_statement = f"insert into dim_curso (tf_curso, curso) values({i+1}, '{r['curso']}')"
#         cursor.execute(insert_statement)
#         conn.commit()

 # ANO
    # dados_ano = pd.DataFrame(dados['NU_ANO_CENSO'].unique(), columns= ['ano'])
    # for i,r in dados_ano.iterrows():
    #     insert_statement = f"insert into dim_ano(tf_ano, ano) values({i+1}, '{r['ano']}')"
    #     cursor.execute(insert_statement)
    #     conn.commit()

    # dados_IES = pd.read_csv('E:/BSI/DS_Aula/Microdados do Censo da Educação Superior 2020/dados/MICRODADOS_CADASTRO_IES_2020.CSV'
    #                     , sep=';'
    #                     , encoding='iso-8859-1'
    #                     , low_memory=False)

    # dados_IES_curso = pd.DataFrame(dados['CO_IES'].unique(), columns= ['co_ies'])
    # for i, r in dados_IES_curso.iterrows():
    #     # determinar o nome da IES
    #     dados_IES_filtrado = dados_IES[dados_IES['CO_IES'] == r['co_ies']]
    #     insert_statement = f"insert into dim_ies (tf_ies, ies) values({i+1}, '{dados_IES_filtrado['NO_IES'].iloc[0]}')"
    #     cursor.execute(insert_statement)
    #     conn.commit()
    
    #Ano
    dados_ano = pd.DataFrame(dados['NU_ANO_CENSO'].unique(), columns = ['ano'])
    for i, r in dados_ano.iterrows():
        insert_statement = f"insert into dim_ano (tf_ano, ano) values({i+1}, '{r['ano']}')"
        cursor.execute(insert_statement)
        conn.commit()
    
    #ies
    dados_IES = pd.read_csv('E:/BSI/DS_Aula/Microdados do Censo da Educação Superior 2020/dados/MICRODADOS_CADASTRO_IES_2020.CSV',
                            sep=';',
                            encoding='iso-8859-1',
                            low_memory=False)
    
    dados_IES_curso = pd.DataFrame(dados['CO_IES'].unique(), columns = ['co_ies'])
    for i, r in dados_IES_curso.iterrows():
        #determinar o nome da ies
        dados_IES_filtrado=dados_IES[dados_IES['CO_IES'] == r['co_ies']]
        no_ies = dados_IES_filtrado['NO_IES'].iloc[0].replace("'" , "")
        insert_statement = f"insert into dim_ies (tf_ies, ies) values({i+1}, '{no_ies}')"
        cursor.execute(insert_statement)
        conn.commit()

except Exception as e:
    print (e)