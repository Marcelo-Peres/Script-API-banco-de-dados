import pyodbc
import pandas as pd

class Connection():

  def __init__(self, user, passwd, db_name, port, server):
    self.user = user
    self.db_name = db_name
    self.port = port
    self.server = server
    self.passwd = passwd
    self.connector = pyodbc.connect(
      'DRIVER={SQL Server};'+
      f'PORT={self.port};'+
      f'SERVER={self.server}'+
      f'DATABASE={self.db_name};'+
      f'UID={self.user};'+
      f'PWD={self.passwd}'
    )

  def connect_sql_server(self):
    return self.connector.cursor()

  def select_programacao_navio(self):
    pd.read_sql_query(
      f'''
      SELECT * FROM CAD_PROGRAMACAONAVIO order by PROGRAMACAONAVIO_ID
      ''', self.connector)


# criando conexão com o banco de dados
# portal_prd = pyodbc.connect(
# 'DRIVER={SQL Server};'+
# 'PORT=1234;'+
# 'SERVER=servidor\prd;'+
# 'DATABASE=portalcliente;'+
# 'UID=meu_usuario;'+
# 'PWD=senha_top#1234'
# )