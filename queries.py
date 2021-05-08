from db_connection import Connection

class Queries(Connection):

  def __init__(self, user, passwd, db_name, port, server):
      super().__init__(user, passwd, db_name, port, server)

  def select_programacao_navio(self):
    return pd.read_sql_query(
      f'''
      SELECT * FROM CAD_PROGRAMACAONAVIO order by PROGRAMACAONAVIO_ID
      ''', self.connector)

  def execute_many(self, new_vessels, novos):
    self.connect_sql_server().executemany(new_vessels, novos)
    return self.connect_sql_server().commit()

  def sql_query(self, set, whare):
    self.connect_sql_server().execute().execute(
      f'''
      UPDATE CAD_PROGRAMACAONAVIO
      SET DATA_ATA = \'{set}\'
      WHERE PROGRAMACAONAVIO_ID = {whare}
      '''
    )
    return self.connect_sql_server().commit()