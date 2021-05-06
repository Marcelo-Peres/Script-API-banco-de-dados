import pyodbc 
import pandas as pd
import numpy as np
import requests
from datetime import datetime

#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)

#função de conexão com uma api para retiradas de dados!
def lineup():
    
    url = 'https://informações_da_api.com.br' # site de acesso exemplo
    bearer = 'informações-de-autenticação'# autenticação por senha 

    headers = {'Authorization' : f'Bearer {bearer}'} # Preparação
    r = requests.get(url, headers=headers) # Requisição
    
    df = pd.read_excel(r.content) # lendo os dados para verificação no banco de dados
    df[['Abertura Gate', 'Dealine', 'ETA', 'ATA', 'ETB', 'ATB', 'ETS', 'ATS']] = df[[
        'Abertura Gate', 'Dealine', 'ETA', 'ATA', 'ETB', 'ATB', 'ETS', 'ATS']].apply(pd.to_datetime, dayfirst=True) # Transformando datas
    df.drop(columns=['Status', 'Largura do Navio'], inplace=True) # excluindo colunas irrelevantes.
    df.columns = ['Berço', 'Navio', 'Viagem', 'Armador', 
                  'Serviço', 'Comprimento(m)', 'Abertura do Gate', 'Deadline', 'ETA',
                  'ATA', 'ETB', 'ATB', 'ETS', 'ATS'] # renomeando colunas para analise
    
    return df
# criando conexão com o banco de dados
portal_prd = pyodbc.connect(
'DRIVER={SQL Server};'+
'PORT=1234;'+
'SERVER=servidor\prd;'+
'DATABASE=portalcliente;'+
'UID=meu_usuario;'+
'PWD=senha_top#1234'
)
# ajuste de objetos para uso
prd_connection_obj: pyodbc.Connection = portal_prd
prd_cursor_obj: pyodbc.Cursor = prd_connection_obj.cursor()
# preparação das informações de inserção
new_vessels = f'''
INSERT INTO CAD_PROGRAMACAONAVIO
(
[DATA_ETA],
[DATA_ETS],
[NAVIO],
[NAVIO_VIAGEM]
)
VALUES(?, ?, ?, ?)
'''

# invocando a função sitada acima
dfe = lineup() 
# conectando ao banco de dados
dfp = pd.read_sql_query(
f'''
SELECT * FROM CAD_PROGRAMACAONAVIO order by PROGRAMACAONAVIO_ID
''', portal_prd)
# unindo informações da função que invoca e trata os dados da api com os dados extratídos do banco de dados
dft = pd.merge(dfe, dfp, how='left', left_on='Viagem', right_on='NAVIO_VIAGEM')
novos = dft[dft.NAVIO_VIAGEM.isna()][['ETA', 'ETS', 'Navio', 'Viagem']] # ajustando dados
novos[['ETA', 'ETS']] = novos[['ETA', 'ETS']].astype(str) # ajustando dados
novos = novos.values.tolist() # ajustando dados

if len(novos) > 0: # usando uma condicional para ver se é ou não necessário inserir dados em banco
    novos_a = f'O total de navio(s) inserido(s) é de {len(novos)}, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'
    prd_cursor_obj.executemany(new_vessels, novos)
    prd_cursor_obj.commit()
else:
    novos_b = f'Sem navios para inserir, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'

# lendo novamente a query do banco
dfp = pd.read_sql_query(
f'''
SELECT * FROM CAD_PROGRAMACAONAVIO order by PROGRAMACAONAVIO_ID
''', portal_prd)
# invocando novamente a função para realizar a atualização
dfe = lineup()
dft = pd.merge(dfe, dfp, how='left', left_on='Viagem', right_on='NAVIO_VIAGEM') # unindo tabelas
dft[['DATA_ATA', 'DATA_ETA', 'DATA_ETS']] = dft[['DATA_ATA', 'DATA_ETA', 'DATA_ETS']].apply(pd.to_datetime, dayfirst=True)

dft['ATA'] = np.where((dft.ATA != dft.DATA_ATA)==True, dft.ATA.astype(str), 'n')
dft['ETA'] = np.where((dft.ETA != dft.DATA_ETA)==True, dft.ETA.astype(str), 'n')
dft['ETS'] = np.where((dft.ETS != dft.DATA_ETS)==True, dft.ETS.astype(str), 'n')
dft[['ATA', 'ETA', 'ETS']] = dft[['ATA', 'ETA', 'ETS']].replace('NaT','n')
# termino de tratamento e validação de inserções
if len(dft[dft.ATA!='n'].ATA) > 0: # etapa atualização 1
    
    ata_total = 0
    
    for i in range(len(dft[dft.ATA!='n'].ATA)):
        prd_cursor_obj.execute(
            f'''
            UPDATE CAD_PROGRAMACAONAVIO
            SET DATA_ATA = \'{str(dft[dft.ATA!='n'].ATA.iloc[i])}\'
            WHERE PROGRAMACAONAVIO_ID = {dft[dft.ATA!='n'].PROGRAMACAONAVIO_ID.iloc[i]}
            '''
        )
        prd_cursor_obj.commit()
        
        ata_total += 1
        
    a = f'O total de registro(s) ATA atualizado(s) é de {ata_total}, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'
        
else:
    b = f'Sem registros ATA para atualizar, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'

if len(dft[dft.ETA!='n'].ETA) > 0: # etapa atualização 2
    
    eta_total = 0
    
    for i in range(len(dft[dft.ETA!='n'].ETA)):
        prd_cursor_obj.execute(
            f'''
            UPDATE CAD_PROGRAMACAONAVIO
            SET DATA_ETA = \'{str(dft[dft.ETA!='n'].ETA.iloc[i])}\'
            WHERE PROGRAMACAONAVIO_ID = {dft[dft.ETA!='n'].PROGRAMACAONAVIO_ID.iloc[i]}
            '''
        )
        prd_cursor_obj.commit()
        
        eta_total += 1
        
    c = f'O total de registro(s) ETA atualizado(s) é de {eta_total}, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'
    
else:
    d = f'Sem registros ETA para atualizar datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'

if len(dft[dft.ETS!='n'].ETS) > 0: # etapa atualização 3
    
    ets_total = 0
    
    for i in range(len(dft[dft.ETS!='n'].ETS)):
        prd_cursor_obj.execute(
            f'''
            UPDATE CAD_PROGRAMACAONAVIO
            SET DATA_ETS = \'{str(dft[dft.ETS!='n'].ETS.iloc[i])}\'
            WHERE PROGRAMACAONAVIO_ID = {dft[dft.ETS!='n'].PROGRAMACAONAVIO_ID.iloc[i]}
            '''
        )
        prd_cursor_obj.commit()
        
        ets_total += 1
    
    e = f'O total de registro(s) ETS atualizado(s) é de {ets_total}, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'

else:
    f = f'Sem registros ETS para atualizar datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'

lista = []

# salvando informações em variáveis para gerar log em um aquivo log.txt
if 'novos_a' in globals():
    lista.append(novos_a)
else:
    lista.append(novos_b)

if 'a' in globals():
    lista.append(a)
else:
    lista.append(b)

if 'c' in globals():
    lista.append(c)
else:
    lista.append(d)

if 'e' in globals():
    lista.append(e)
else:
    lista.append(f)

# lista final

f=open('log.txt','w') # criando arquivo
text='\n'.join(lista) # criando o texto a ser escrito
f.write(text) # escrevendo no arquivo
f.close()# finalizando o script
