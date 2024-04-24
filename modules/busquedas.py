import pandas as pd
import requests
import time
import urllib.parse
import os
import requests
from bs4 import BeautifulSoup
from modules.oracle import OracleDataInserter

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
BERA_CONEXION = "karstec"
BERA_ESQUEMA = "KARSTEC"
DEV_GLUE_CONN = "oracle_datos_clientes_creds"
DEV_ORACLE_SCHEMA = "DATOS_CLIENTES"
bera_con = OracleDataInserter(schema=BERA_ESQUEMA, glue_con=BERA_CONEXION)
dev_con = OracleDataInserter(schema=DEV_ORACLE_SCHEMA, glue_con=DEV_GLUE_CONN)

def primer_busqueda_y_guardado(df_listado):
    resultados_cuit = []

    # Crear un DataFrame para almacenar los resultados
    df_resultados = pd.DataFrame(columns=['nombre', 'apellido', 'cuit'])
    
    for i in range(0, len(df_listado), 10):
        lotes = df_listado.iloc[i:i+10]  # Obtener el lote actual
        nombres_apellidos = []  # Lista para almacenar nombres y apellidos del lote actual
        
        for index, row in lotes.iterrows():
            partes = row['RESP_PAGO'].split(" ")
            nombre = partes[0]
            apellido = " ".join(partes[1:])
            nombres_apellidos.append((nombre, apellido))

        for nombre, apellido in nombres_apellidos:
            url = 'https://www.cuitonline.com/search.php?q=' + apellido + '%20' + nombre
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            resultados = soup.find_all('div', class_='hit')

            if resultados:
                cuits = []
                for resultado in resultados:
                    cuit_elem = resultado.find('span', class_='cuit')
                    if cuit_elem:
                        cuit = cuit_elem.text.strip().replace('-', '')
                        cuits.append(cuit)
                time.sleep(5)

                if cuits:
                    for cuit in cuits:
                        resultados_cuit.append({"nombre": nombre, "apellido": apellido, "cuit": cuit})
                else:
                    resultados_cuit.append({"nombre": nombre, "apellido": apellido, "cuit": 'No encontrado'})

            else:
                resultados_cuit.append({"nombre": nombre, "apellido": apellido, "cuit": 'No encontrado'})

    # Llenar el DataFrame de resultados
    df_resultados = pd.DataFrame(resultados_cuit)

    df_resultados.rename(columns=lambda x: x.upper(), inplace=True)
   
    return df_resultados

def busqueda_final(df_resultados):
    datos_finales = []  # Lista para acumular los datos recolectados
    
    # Dividir el DataFrame en lotes de 10 filas
    num_lotes = len(df_resultados) // 10 + (1 if len(df_resultados) % 10 > 0 else 0)
    print(f'Se armaron {num_lotes} lotes para la búsqueda final.')
    for i in range(0, len(df_resultados), 10):
        lotes = df_resultados.iloc[i:i+10]  # Obtener el lote actual
        for index, row in lotes.iterrows():
            nombre = row['NOMBRE']
            apellido = row['APELLIDO']
            cuit = row['CUIT']

            if cuit:  # Solo intenta la búsqueda si el CUIT existe
                apellido_codificado = urllib.parse.quote(apellido)  # Codificar el apellido
                url = f'https://www.cuitonline.com/detalle/{cuit}/{apellido_codificado}-{nombre}.html'
                try:
                    response = requests.get(url)
                    response.raise_for_status()  # Asegura que no haya errores en la respuesta
                except Exception as e:
                    print(f"Error al obtener la respuesta: {e}")
                encoding = response.encoding  
                response_content = response.content.decode(encoding)

                time.sleep(5)

                soup = BeautifulSoup(response_content, 'html.parser')
                persona_data = soup.find('div', class_='persona-data')
                if persona_data:
                    provincia_elem = persona_data.find('span', itemprop='addressRegion')
                    localidad_elem = persona_data.find('span', itemprop='addressLocality')
                    # Verifica si los elementos de provincia y localidad existen antes de acceder a sus atributos
                    provincia = provincia_elem.text.strip() if provincia_elem else "No encontrado"
                    localidad = localidad_elem.text.strip() if localidad_elem else "No encontrado"
                    datos_finales.append({"nombre": nombre, "apellido": apellido, "cuit": cuit, "provincia": provincia, "localidad": localidad})
                else:
                    datos_finales.append({"nombre": nombre, "apellido": apellido, "cuit": cuit, "provincia": "No encontrado", "localidad": "No encontrado"})

        print(f'SEGUNDA BUSQUEDA CUIT ONLINE: PROCESANDO LOTE {i//10 + 1} de {num_lotes}')

    return pd.DataFrame(datos_finales)