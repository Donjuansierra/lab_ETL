import pandas as pd
import requests
from typing import Set
import sqlite3
import json


#Carga los datos demográficos

def ej_1_cargar_datos_demograficos() -> pd.DataFrame:
    datos_demograficos = pd.DataFrame()
    url = "https://public.opendatasoft.com/explore/dataset/us-cities-demographics/download/?format=csv&timezone=America/New_York&use_labels_for_header=true"
    datos_demograficos = pd.read_csv(url, sep=';')
    return datos_demograficos

#Extraer los datos de calidad del aire

def ej_2_cargar_calidad_aire(data: pd.DataFrame) -> None:
    resultado = []
    overall_aqit = []
    city_list = datos_demograficos['City']
    city_lit=[]
    for city in city_list:
        api_url = f'https://api.api-ninjas.com/v1/airquality?city={city}'
        response = requests.get(api_url, headers={'X-Api-Key': 'tnAXTjhGJUF9FCVvRZqenQ==A8xoElsrHq8rJZ3h'})
        dimensiones = []
        if response.status_code == requests.codes.ok:
            city_res=response.json()
            for clave, valor in city_res.items():
                if clave != "overall_aqi":
                    if clave != "overall_aqi":
                        del valor['aqi']
                        dimensiones.append(valor['concentration'])
            if clave == "overall_aqi":
                overall_aqi= city_res['overall_aqi']
                overall_aqit.append(overall_aqi)
            resultado.append(dimensiones)
            city_lit.append(city)
        else:
            print("Error:", response.status_code, response.text)
    city_lit= pd.DataFrame(city_lit)
    city_lit = city_lit.rename(columns={0:'City'})
    resultado = pd.DataFrame(resultado)
    resultado = resultado.rename(columns={0:'CO', 1:'NO2', 2:'O3', 3:'SO2', 4:'PM2.5', 5:'PM10'})
    overall_aqit = pd.DataFrame(overall_aqit)
    overall_aqit = overall_aqit.rename(columns={0:'overall_aqi'})
    base = pd.concat([city_lit, resultado, overall_aqit], axis=1)
    base.set_index('City', inplace=True)
    base.to_csv('ciudades.csv')
    return base


datos_demograficos = ej_1_cargar_datos_demograficos()
ciudades = set(datos_demograficos['City'].to_list())
ciudades_lista = list(ciudades)
calidad_aire_df = ej_2_cargar_calidad_aire(ciudades_lista)


# Se realiza la limpieza de los datos demograficos

poblacion= datos_demograficos.drop(columns=["Race", "Count", "Number of Veterans"]).drop_duplicates().reset_index(drop=True)
poblacion.to_csv('poblacion.csv', index=False)

# Se realiza el cargue de los CSV de población y ciudades en una base de datos para su posterior analisis

ciudades = pd.read_csv('ciudades.csv')
poblacion = pd.read_csv('poblacion.csv')
conn = sqlite3.connect('analisis_de_aire.db')
ciudades.to_sql('ciudades', conn, if_exists='replace', index=False)
poblacion.to_sql('poblacion', conn, if_exists='replace', index=False)
conn.close()


# se realiza la consulta 
conn = sqlite3.connect('analisis_de_aire.db')
cursor = conn.execute("SELECT poblacion.city, poblacion.'Total population', ciudades.'overall_aqi' FROM poblacion INNER JOIN ciudades ON poblacion.city = ciudades.city ORDER BY  'Total population' DESC LIMIT 10; ")
filas = cursor.fetchall()
for fila in filas:
    print(fila)


