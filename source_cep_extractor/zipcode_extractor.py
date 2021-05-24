import time
import requests
import pandas as pd
import s3fs

ZIPCODES_DATABASE_URL = "s3://cinnecta-tests/marrinha/cep_lat_long_database.csv"
TOKEN = "1ca46a80c8b68a3a86d2e7610ceba033"


class ZipcodeExtractor:
  zipcode_database = None

  def __init__(self):
    self.zipcode_database = pd.read_csv(ZIPCODES_DATABASE_URL)

  def update_database(self, searched_data):
    self.zipcode_database.append(searched_data, ignore_index=True)
    self.zipcode_database.to_csv(ZIPCODES_DATABASE_URL)

  def search_for_zipcode(self, zipcode):
    current = self.zipcode_database[self.zipcode_database['cep'] == zipcode]
    if len(current.index) is not 0:
      return {
        'cep': zipcode,
        'latitude': current['latitude'].values[0],
        'longitude': current['longitude'].values[0]
      }

    headers = {'Authorization': f'Token token={TOKEN}'}
    response = requests.get(f"https://www.cepaberto.com/api/v3/cep?cep={zipcode}", headers=headers)
    data = response.json()

    searched_data = {
      'cep': zipcode,
      'latitude': data['latitude'],
      'longitude': data['longitude']
    }

    time.sleep(1) #obrigado esperar entre as requests
    #TODO
    #pronto para agregar novos dados a base csv
    #self.update_database(searched_data)
    return searched_data
