import pyodbc 
import json
import pandas as pd
#teste
import queries
from env import CONX_STRING,CONX_STRING_ETL, env

# ---- OBJETO DE CONEXAO ----
#define a conexão para o banco
#conn = pyodbc.connect(CONX_STRING)

#conn_etl = pyodbc.connect(CONX_STRING_ETL)

nocount = """ SET NOCOUNT ON; """

def exec_query_prod(query): 
    conn = pyodbc.connect(CONX_STRING)
    conn.cursor()

    df_result = pd.read_sql(nocount + query, conn)
    conn.close()
    return df_result


def get_id_cr(idcrs): 
    try:
        conn = pyodbc.connect(CONX_STRING)
        conn.cursor()

        # Verifica se idcrs é uma lista ou um único valor
        if isinstance(idcrs, list):
            idcrs_str = ",".join(str(idcr) for idcr in idcrs)
            idcrs_formatted = f"({idcrs_str})"
        else:
            idcrs_formatted = f"({idcrs})"
        
        query = queries.get_id_cr.format(idcrs_formatted)
        
        df_result = pd.read_sql(nocount + query, conn)
        conn.close()

        return df_result
    except Exception as e:
        raise e


def get_Rotina_Por_Estrutura(idEstrutura):
    try:
        conn = pyodbc.connect(CONX_STRING)
        conn.cursor()
        
        df_result = pd.read_sql(nocount + queries.get_Rotina_Por_Estrutura_certo.format(idEstrutura), conn)
        conn.close()
        return df_result
    except Exception as e:
        raise e


    