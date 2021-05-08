from autentication import get_porto_api
from queries import Queries
import pandas as pd
import numpy as np
from datetime import datetime


#função de conexão com uma api para retiradas de dados!
def lineup():
    df = pd.read_excel(get_porto_api()) # lendo os dados para verificação no banco de dados
    df[['Abertura Gate', 'Dealine', 'ETA', 'ATA', 'ETB', 'ATB', 'ETS', 'ATS']] = df[[
        'Abertura Gate', 'Dealine', 'ETA', 'ATA', 'ETB', 'ATB', 'ETS', 'ATS']].apply(pd.to_datetime, dayfirst=True) # Transformando datas
    df.drop(columns=['Status', 'Largura do Navio'], inplace=True) # excluindo colunas irrelevantes.
    df.columns = ['Berço', 'Navio', 'Viagem', 'Armador', 
                  'Serviço', 'Comprimento(m)', 'Abertura do Gate', 'Deadline', 'ETA',
                  'ATA', 'ETB', 'ATB', 'ETS', 'ATS'] # renomeando colunas para analise
    
    return df

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

connect_with_sara = Queries('user', 'pass', 'db', 5005, '127.0.0.1')

prd_cursor_obj = connect_with_sara.connect_sql_server()

# invocando a função sitada acima
dfe = lineup() 

# conectando ao banco de dados
dfp = connect_with_sara.select_programacao_navio()

# unindo informações da função que invoca e trata os dados da api com os dados extratídos do banco de dados
dft = pd.merge(dfe, dfp, how='left', left_on='Viagem', right_on='NAVIO_VIAGEM')
novos = dft[dft.NAVIO_VIAGEM.isna()][['ETA', 'ETS', 'Navio', 'Viagem']] # ajustando dados
novos[['ETA', 'ETS']] = novos[['ETA', 'ETS']].astype(str) # ajustando dados
novos = novos.values.tolist() # ajustando dados

if len(novos) > 0: # usando uma condicional para ver se é ou não necessário inserir dados em banco
    novos_a = f'O total de navio(s) inserido(s) é de {len(novos)}, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'
    prd_cursor_obj.execute_many(new_vessels, novos)
else:
    novos_b = f'Sem navios para inserir, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'

# lendo novamente a query do banco
dfp = connect_with_sara.select_programacao_navio()
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
        prd_cursor_obj.sql_query(
          str(dft[dft.ATA!='n'].ATA.iloc[i]),
          dft[dft.ATA!='n'].PROGRAMACAONAVIO_ID.iloc[i]
        )
        
        ata_total += 1
        
    a = f'O total de registro(s) ATA atualizado(s) é de {ata_total}, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'
        
else:
    b = f'Sem registros ATA para atualizar, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'

if len(dft[dft.ETA!='n'].ETA) > 0: # etapa atualização 2
    
    eta_total = 0
    
    for i in range(len(dft[dft.ETA!='n'].ETA)):
        prd_cursor_obj.sql_query(
          str(dft[dft.ETA!='n'].ETA.iloc[i]),
          dft[dft.ETA!='n'].PROGRAMACAONAVIO_ID.iloc[i]
        )
        
        eta_total += 1
        
    c = f'O total de registro(s) ETA atualizado(s) é de {eta_total}, datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'
    
else:
    d = f'Sem registros ETA para atualizar datado em {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}!'

if len(dft[dft.ETS!='n'].ETS) > 0: # etapa atualização 3
    
    ets_total = 0
    
    for i in range(len(dft[dft.ETS!='n'].ETS)):
        prd_cursor_obj.sql_query(
          str(dft[dft.ETS!='n'].ETS.iloc[i]),
          dft[dft.ETS!='n'].PROGRAMACAONAVIO_ID.iloc[i]
        )
        
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
