import calendar
import os
import sys
import requests
import pandas as pd
import pyodbc 
import numpy as np
import warnings
from datetime import date, datetime, timedelta
import json
import queries
import env
import db
import time
import json


##todo
#bug ao vincular rotina pra retroativo, a rotina nao vai pegar a data do inicio do proprio mes, e sim jogar pro proximo, colocar uma opcao pra quando for
    #retroativo vincular rotina sem gerar a tarefa, e gerar manual pro comeco do mes
#trazer como df todas as tarefas de visita, pra fazer a verificacao retroativa
#rotinas auxiliares
    #job para apagar tarefas e rotinas de crs de visita inativos, ou que o negocio nao condiz com o criterio.
warnings.filterwarnings('ignore')

urlEnv = env.baseUrl

#Todas estruturas não podem ter mais de uma rotina de Visita - Ao vincular uma nova Rotina deletar a Rotina e as tarefas em aberto da rotina Antiga

def getToken():
    url = f"https://..."
    header = {
        'Content-Type': "application/json",
    }
    payload = {
        "cpf": "...",
        "password": "..."
    }   
    res = requests.post(url, data = json.dumps(payload), headers = header)
    return json.loads(res.content)["authToken"]

def buscarColaborador():
    try: 
        today = datetime.today()      
        data_30 = (today - timedelta(days=30)).strftime("%Y%m%d")
        data_60 = (today - timedelta(days=60)).strftime("%Y%m%d")
        data_90 = (today - timedelta(days=90)).strftime("%Y%m%d")
        token = f"..."
        header = {
                'Content-Type': "application/json",
                'authenticationToken': token,
            }     
        url = "..."        
        payload30 = {          
                "admissao": f"""{data_30}"""
            }
        payload60 = {          
                "admissao": f"""{data_60}"""
            }
        payload90 = {          
                "admissao": f"""{data_90}"""
            }
        response30 = requests.post(url, data = json.dumps(payload30), headers = header)
        response60 = requests.post(url, data = json.dumps(payload60), headers = header)
        response90 = requests.post(url, data = json.dumps(payload90), headers = header)

        json_response30 = response30.json()
        json_response60 = response60.json()
        json_response90 = response90.json()

        cr_30 = []
        cr_60 = []
        cr_90 = []

        for item in json_response30:
            cr_value = item.get("cr")
            if cr_value is not None:
                cr_30.append(cr_value)

        for item in json_response60:
            cr_value = item.get("cr")
            if cr_value is not None:
                cr_60.append(cr_value)

        for item in json_response90:
            cr_value = item.get("cr")
            if cr_value is not None:
                cr_90.append(cr_value)   

        cr_30 = list(set(cr_30))
        cr_60 = list(set(cr_60))
        cr_90 = list(set(cr_90))                             

        cr_list = [cr_30, cr_60, cr_90]
        cr_poc = ['31434','32766','32769','32767','32768','31382',
                  '32763','31384','31383','32765','32761','31435']

        matches = [x for x in cr_list if x in cr_poc]

        print(matches)

        return matches
    except Exception as e:
        writeLog("Erro:" + e.args[0])

def writeLog(logMessage):
    today = date.today()
    now = datetime.now()

    logText = f"""{today.strftime("%Y-%m-%d")} {now.strftime("%H:%M:%S")} - {logMessage}""" 

    with open(os.getcwd() + '\\log.txt', 'a') as f:
        f.write('\n' + logText)


def validarRotina(idEstrutura):
    return db.get_Rotina_Por_Estrutura_certo(idEstrutura)  




def vincularRotinas(idRotina,estruturaId):    
    token = f"Bearer {getToken()}" 
    url = f"https://{urlEnv}...{idRotina}&estruturaId={estruturaId}"

    header = {
        'Content-Type': "application/json",
        'Authorization': token,
    }      
    res = requests.post(url, headers = header)  
    writeLog('Estrutura:' + estruturaId+ ' - rotina vinculada com sucesso')                        
    writeLog("Estrutura: " + estruturaId + " - Status: " + str(res.status_code))  

   

def executar_main():
    df_get_id_cr = db.get_id_cr(buscarColaborador())
    try:
        for index,row in  df_get_id_cr.iterrows():      
            idEstrutura = row['id']
            df_get_Rotina_Por_Estrutura = db.get_Rotina_Por_Estrutura()    
            idRotina = df_get_Rotina_Por_Estrutura['id'].iloc[0]
            dfEstrutura = validarEstrutura(idEstrutura)
            if len(dfEstrutura.values): 
                dfRotina = validarRotina(idEstrutura)  

                #Não tem rotina vinculada a essa estrutura
                if dfRotina.empty: 
                    vincularRotinas(idRotina,idEstrutura)                   
                #Rotina vinculada, porém sem tarefa no mes seguinate
                else:
                    writeLog(f'{row[1]} - ja possui rotina')                        
                    adicionarTarefa(idRotina)                        

    except Exception as e:            
        writeLog("Erro: " + e.args[0] + " - Estrutura: " + row['id'])
        

def ativarEstrutura(id):
    try:
        token = f"Bearer {getToken()}"
        header = {
            'Content-Type': "application/json",
            'Authorization': token,
        }     
        url = f"https://{urlEnv}...?estruturaId={id}"
        response = requests.get(
            url, 
            headers = header
        )  
        if response.status_code == 204 or response.status_code == 200:
            return True
        else: raise e
    except Exception as e:      
        raise e
    
def adicionarTarefa(idRotina):
    try:       
        data_formatada = datetime.now().strftime("%Y-%m-%dT00:00:00.000Z")
        token = f"Bearer {getToken()}"
        header = {
            'Content-Type': "application/json",
            'Authorization': token,
        }     
        url = f"https://{urlEnv}.../adicionar-tarefa-integracao"        
        payload = {
            "rotinaId": idRotina,            
            "data": f"""{data_formatada}"""
            
        }
        response = requests.post(url, data = json.dumps(payload), headers = header)
        writeLog(f'rotina: {idRotina} - tarefa criada para data {data_formatada}')  
        #return None
        return response.text.replace('"','')
    except Exception as e:
        writeLog("Erro: " + e.args[0] + " - Rotina: " + idRotina)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        writeLog(f'Add task error {e}: {exc_type, fname, exc_tb.tb_lineno}')         
    
   

def validarEstrutura(idEstrutura):
    dfEstrutura = db.get_id_cr(idEstrutura)
    if len(dfEstrutura.values):
        if dfEstrutura['Status'].values[0] == 2:
            ativarEstrutura(dfEstrutura['Id'].values[0])
    
    return dfEstrutura
  
executar_main()