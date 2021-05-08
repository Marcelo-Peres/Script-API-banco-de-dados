import requests

def get_porto_api():
  url = 'https://informações_da_api.com.br' # site de acesso exemplo
  bearer = 'informações-de-autenticação'# autenticação por senha 

  headers = {'Authorization' : f'Bearer {bearer}'} # Preparação
  return requests.get(url, headers=headers) # Requisição