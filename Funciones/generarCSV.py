import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")
from pandas import DataFrame
import pandas as pd
import json
import os
import Funciones.generales
import numpy as np

provincia_map = {
    0: "CIUDAD AUTONOMA BUENOS AIRES",
    1: "BUENOS AIRES",
    2: "CATAMARCA",
    3: "CORDOBA",
    4: "CORRIENTES",
    5: "ENTRE RIOS",
    6: "JUJUY",
    7: "MENDOZA",
    8: "LA RIOJA",
    9: "SALTA",
    10: "SAN JUAN",
    11: "SAN LUIS",
    12: "SANTA FE",
    13: "SANTIAGO DEL ESTERO",
    14: "TUCUMAN",
    16: "CHACO",
    17: "CHUBUT",
    18: "FORMOSA",
    19: "MISIONES",
    20: "NEUQUEN",
    21: "LA PAMPA",
    22: "RIO NEGRO",
    23: "SANTA CRUZ",
    24: "TIERRA DEL FUEGO"
}


def eliminarColumna(diccionario,nombreColumna):
    diccionario.pop(nombreColumna)

def agregarColumnaVacia(diccionario, nombreColumna):
    diccionario[nombreColumna] = ""

def existeColumna(diccionario,nombreColumna):
    if nombreColumna not in diccionario:
        agregarColumnaVacia(diccionario,nombreColumna)
    if nombreColumna == False:
        diccionario[nombreColumna] = ""

def validacionColumnasTelefonosDeclarados(diccionario):
    existeColumna(diccionario,'numero')

def validacionColumnasCelulares(diccionario):
    existeColumna(diccionario,'numero')

def validacionColumnasTelefonosFijos(diccionario):
    existeColumna(diccionario,'telefono')

def validacionColumnasMails(diccionario):
    existeColumna(diccionario, 'mail')

def validacionColumnasMailsAfip(diccionario):
    existeColumna(diccionario, 'direccion')

def validacionVacioJson(diccionario):
    if diccionario == "":
        return False


def validacionColumnasDomicilioInfoExperto(diccionario):
    existeColumna(diccionario,'calle')
    existeColumna(diccionario,'altura')
    existeColumna(diccionario,'extra')
    existeColumna(diccionario,'barrio')
    existeColumna(diccionario,'localidad')
    existeColumna(diccionario,'provincia')
    existeColumna(diccionario,'cp')
    existeColumna(diccionario,'tipo')
    existeColumna(diccionario,'fecha')

def validacionColumnasDomicilioAfip(diccionario):
    existeColumna(diccionario,'codPostal')
    existeColumna(diccionario,'direccion')
    existeColumna(diccionario,'localidad')
    existeColumna(diccionario,'idProvincia')
    existeColumna(diccionario,'tipoDomicilio')
    existeColumna(diccionario,'descripcionProvincia')

def reemplazar_por_provincia(valor):
    if type(valor) == int:
        return provincia_map.get(valor, valor)
    return valor

def guardarIdentidades(jsons, pathDirectorioJsons, pathDirectorioIdentidades):
    print("guardarIdentidades en Ejecucion")
    finalDataFrame = DataFrame()
    for archivo in jsons:
        filepath = os.path.join(pathDirectorioJsons + '\\' + archivo)
        file = open(filepath)
        data = json.load(file)
        datos_json = data["datos"]["identidad"]
        df1 = pd.DataFrame([datos_json])
        finalDataFrame = finalDataFrame.append(df1)
        print("CUIT: ", datos_json["cuit"])
    finalDataFrame.to_csv(pathDirectorioIdentidades+'\\'+"identidades"+'.csv', index = None)
    print("guardarIdentidades Finalizado")

def guardarTelefonos(jsons,pathDirectorioJsons,pathDirectorioTelefonos):
    print("guardarTelefonos en Ejecucion")
    finalDataFrame = DataFrame()
    for archivo in jsons:
        filepath = os.path.join(pathDirectorioJsons + '\\' + archivo)
        file = open(filepath)
        data = json.load(file)
        datos_json_celulares = data["datos"]["celulares"]
        datos_json_telefonosDeclarados = data["datos"]["telefonosDeclarados"]
        datos_json_telefonosFijos = data["datos"]["telefonos"]
        #dfTelefonosFijosAfip = data["datos"]["soaAfipA4Online"]["telefono"]
        cuit = data["datos"]["identidad"]["cuit"]
        print(archivo.replace(".json",""))
        if datos_json_celulares != False:
            for telefono in datos_json_celulares:
                telefono['cuit'] = cuit
                validacionColumnasCelulares(telefono)
                df1 = pd.DataFrame([telefono])
                df1 = df1[['cuit','numero']]
                finalDataFrame = finalDataFrame.append(df1)
        if datos_json_telefonosDeclarados != False:
            for telefono in datos_json_telefonosDeclarados:
                telefono['cuit'] = cuit
                validacionColumnasTelefonosDeclarados(telefono)
                df2 = pd.DataFrame([telefono])
                df2 = df2[['cuit','numero']]
                finalDataFrame = finalDataFrame.append(df2)
        if datos_json_telefonosFijos != False:
            for telefono in datos_json_telefonosFijos:
                telefono['cuit'] = cuit
                if 'nombre' in telefono:
                    telefono.pop('nombre')
                if 'domicilio' in telefono:
                    telefono.pop('domicilio')
                if 'cp' in telefono:
                    telefono.pop('cp')
                if 'localidad' in telefono:
                    telefono.pop('localidad')
                if 'provincia' in telefono:
                    telefono.pop('provincia')
                if 'guia' in telefono:
                    telefono.pop('guia')
                validacionColumnasTelefonosFijos(telefono)
                
                df1 = pd.DataFrame([telefono])
                df1.columns = ['numero','cuit']
                df1 = df1[['cuit','numero']]
                finalDataFrame = finalDataFrame.append(df1)
        #datos_json_domicilios_afip = data["datos"]["soaAfipA4Online"]["domicilio"]
        #df2 = pd.DataFrame([datos_json_domicilios_afip])
        #finalDataFrame = finalDataFrame.append(df2)
    finalDataFrame.to_csv(pathDirectorioTelefonos+'\\'+"telefonos"+'.csv', index = None)
    print("guardarTelefonos Finalizado")

def guardarMails(jsons, pathDirectorioJsons, pathDirectorioMails):
    print("guardarMails en Ejecucion")
    finalDataFrame = DataFrame()
    for archivo in jsons:
        filepath = os.path.join(pathDirectorioJsons + '\\' + archivo)
        file = open(filepath)
        data = json.load(file)
        datos_json_mails = data["datos"]["mails"]
        if data["datos"]["soaAfipA4Online"] == False:
            datos_json_mails_afip = False
        else:
            datos_json_mails_afip = data["datos"]["soaAfipA4Online"]["email"]

        cuit = data["datos"]["identidad"]["cuit"]
        print(archivo.replace(".json",""))
        if datos_json_mails != False and datos_json_mails != "" and datos_json_mails != "false" :
            for mail in datos_json_mails:
                mail['cuit'] = cuit
                mail['tipoEmail'] = ""
                if 'estado' in mail:
                    mail.pop('estado')
                validacionColumnasMails(mail)
                print("Mail: "+str(mail))
                df1 = pd.DataFrame([mail])
                df1.columns =['mail','cuit','tipoEmail']
                df1 = df1[['cuit','mail','tipoEmail']]
                finalDataFrame = finalDataFrame.append(df1)
        if datos_json_mails_afip != False and datos_json_mails_afip != "" and datos_json_mails_afip != "false" :
            for mail in datos_json_mails_afip:
                mail['cuit'] = cuit
                if 'estado' in mail:
                    mail.pop('estado')
                validacionColumnasMailsAfip(mail)
                print("Mail Afip: "+str(mail))
                df2 = pd.DataFrame([mail])
                df2.columns = ['mail','tipoEmail','cuit']
                df2 = df2[['cuit','mail','tipoEmail']]
                finalDataFrame = finalDataFrame.append(df2)
    finalDataFrame.to_csv(pathDirectorioMails+'\\'+"mails"+'.csv', index = None)
    print("guardarMails Finalizado")


def guardarDomicilios(jsons, pathDirectorioJsons, pathDirectorioDomicilios):
    print("guardarDomicilios en Ejecucion")
    finalDataFrame = DataFrame()
    for archivo in jsons:
        filepath = os.path.join(pathDirectorioJsons + '\\' + archivo)
        file = open(filepath)
        data = json.load(file)
        datos_json_domicilios = data["datos"]["domicilios"]
        if data["datos"]["soaAfipA4Online"] == False:
            datos_json_domicilios_afip = False
        else:
            datos_json_domicilios_afip = data["datos"]["soaAfipA4Online"]["domicilio"]
        cuit = data["datos"]["identidad"]["cuit"]
        print(archivo.replace(".json",""))
        if datos_json_domicilios != False:
            for domicilio in datos_json_domicilios:
                domicilio['tipoOrigen'] = 'PrincipalInfoExperto'
                domicilio['cuit'] = cuit
                domicilio['descripcionProvincia'] = ""
                validacionColumnasDomicilioInfoExperto(domicilio)
                df1 = pd.DataFrame([domicilio])
                df1 = df1[['cuit','calle','altura','extra','barrio','localidad','provincia','cp','tipo','fecha','descripcionProvincia','tipoOrigen']]
                finalDataFrame = finalDataFrame.append(df1)
        if datos_json_domicilios_afip != False:
            for domicilioAfip in datos_json_domicilios_afip:
                domicilioAfip['tipoOrigen'] = 'AFIP'
                domicilioAfip['cuit'] = cuit
                domicilioAfip['altura'] = ""
                domicilioAfip['extra'] = ""
                domicilioAfip['barrio'] = ""
                domicilioAfip['fecha'] = ""
                if 'tipoDatoAdicional' in domicilioAfip:
                    domicilioAfip.pop('tipoDatoAdicional')
                if 'datoAdicional' in domicilioAfip:
                    domicilioAfip.pop('datoAdicional')
                validacionColumnasDomicilioAfip(domicilioAfip)
                df2 = pd.DataFrame([domicilioAfip])
                if df2.columns[-1] == "localidad":
                    df2.columns = ['cp','calle','provincia','tipo','descripcionProvincia','tipoOrigen','cuit','altura','extra','barrio','fecha','localidad']
                else:        
                    df2.columns = ['cp','calle','localidad','provincia','tipo','descripcionProvincia','tipoOrigen','cuit','altura','extra','barrio','fecha']
                df2 = df2[['cuit','calle','altura','extra','barrio','localidad','provincia','cp','tipo','fecha','descripcionProvincia','tipoOrigen']]
                #df2 = df2[['cuit','calle','altura','extra','barrio','localidad','provincia','cp','tipo','fecha','descripcionProvincia','tipoOrigen']]
                finalDataFrame = finalDataFrame.append(df2)
        #datos_json_domicilios_afip = data["datos"]["soaAfipA4Online"]["domicilio"]
        #df2 = pd.DataFrame([datos_json_domicilios_afip])
        #finalDataFrame = finalDataFrame.append(df2)
    if 'provincia' in finalDataFrame.columns:
        finalDataFrame['provincia'] = finalDataFrame['provincia'].apply(reemplazar_por_provincia)
        finalDataFrame.to_csv(pathDirectorioDomicilios+'\\'+"domicilios"+'.csv', index = None)
    print("guardarDomicilios Finalizado")

def guardarRodados(jsons, pathDirectorioJsons,pathDirectorioRodados):
    print("guardarRodados en Ejecucion")
    finalDataFrame = DataFrame()
    for archivo in jsons:
        filepath = os.path.join(pathDirectorioJsons + '\\' + archivo)
        file = open(filepath)
        data = json.load(file)
        datos_json_rodados = data["datos"]["rodados"]
        if "dominioBuscado" in data["datos"]:
            datos_json_rodado_buscado = data["datos"]["dominioBuscado"]
        else:
            datos_json_rodado_buscado = False
        cuit = data["datos"]["identidad"]["cuit"]
        print(archivo)
        if datos_json_rodados != False:
            for rodado in datos_json_rodados:
                rodado['cuit'] = cuit #archivo.replace(".json","")
                df1 = pd.DataFrame([rodado])
                df1 = df1[['cuit','dominio','origen','marca','version','modelo','porcentaje','fecha_transaccion']]
                df1['fecha_transaccion'] = np.where(df1['fecha_transaccion'].str.contains('/'),
                      df1['fecha_transaccion'],
                      pd.to_datetime(df1['fecha_transaccion'], format='%Y-%m-%d', errors='coerce').dt.strftime('%d/%m/%Y'))
                finalDataFrame = finalDataFrame.append(df1)
        if datos_json_rodado_buscado != False:
            datos_json_rodado_buscado['cuit'] = cuit
            df2 = pd.DataFrame([datos_json_rodado_buscado])
            df2 = df2[['cuit','dominio','origen','marca','version','modelo','porcentaje','fecha_transaccion']]
            df2['fecha_transaccion'] = np.where(df2['fecha_transaccion'].str.contains('/'),
                      df2['fecha_transaccion'],
                      pd.to_datetime(df2['fecha_transaccion'], format='%Y-%m-%d', errors='coerce').dt.strftime('%d/%m/%Y'))
              
            finalDataFrame = finalDataFrame.append(df2)

    if 'dominio' in finalDataFrame.columns:
        finalDataFrame['tipo'] = finalDataFrame['Tipo'] = finalDataFrame['dominio'].apply(Funciones.generales.tipoVehiculo)
        finalDataFrame.to_csv(pathDirectorioRodados+'\\'+"rodados"+'.csv', index = None)
   
    print("guardarRodados Finalizado")