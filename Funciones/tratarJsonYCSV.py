import json
import csv
import os

def guardarJson(jsonCapturado, cuit, pathDirectorio):
    filepath = os.path.join(pathDirectorio + '/' + cuit + ".json")
    with open(filepath, 'w') as file:
        json.dump(jsonCapturado, file)

def csvALista(archivoListadoCSV):
    print("csvALista en Ejecucion")
    with open(archivoListadoCSV,'r') as file:
        reader = csv.reader(file)
        listado = list(reader)
    N = [",".join([str(x) for x in m]) for m in listado]
    print(N)
    print("csvALista Finalizado")
    return N

def listarArchivosEnLista(pathDirectorio):
    print("listarArchivosEnLista en Ejecucion")

    listaArchivos = os.listdir(pathDirectorio)
    listadoConDatos  = []
    for archivo in listaArchivos:
        filepath = os.path.join(pathDirectorio + '/' + archivo)
        file = open(filepath)
        data = json.load(file)
        if data["estado"] == "OK":
            listadoConDatos.append(archivo)
    print("listarArchivosEnLista Finalizado")
    return listadoConDatos

def quitar_json(s):
  return s.replace(".json", "")

