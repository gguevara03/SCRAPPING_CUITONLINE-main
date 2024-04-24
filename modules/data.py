import datetime
import json
import logging
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Literal

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def tipo_vehiculo(dominio) -> Literal["M", "A"]:
    if dominio[:3].isnumeric() or dominio[1:4].isnumeric():
        return "M"
    else:
        return "A"

class DataScrapping:
    def extract_identidades(self, df, processing_time, s3_key=None) -> None:
        try:
            df_list = []

            for j_row, vista in zip(df[0], df['vista']):
                if j_row:
                    json_data = json.loads(j_row)
                    if json_data.get("estado") == "OK":
                        cuit = json_data["datos"]["identidad"]["cuit"]
                        ie_data = pd.json_normalize(
                            json_data["datos"]["identidad"])
                        ie_data["cuit"] = cuit
                        ie_data.drop(
                            ["localidad_array", "provincia_array"],
                            axis=1,
                            inplace=True,
                            errors="ignore",
                        )
                        df_list.append(ie_data)

            if len(df_list) == 0:
                logger.warning("No data to extract")
                return pd.DataFrame()

            final_df = pd.concat(df_list, ignore_index=True, axis=0)
            final_df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
            final_df.drop_duplicates(inplace=True)
            final_df["processing_time"] = processing_time
            final_df["vista"] = vista

            # refactorizacion data wrangling
            final_df['fecha_nacimiento'] = pd.to_datetime(
                final_df['fecha_nacimiento'], errors='coerce', format="%d/%m/%Y")
            final_df['fecha_inscripcion'] = pd.to_datetime(
                final_df['fecha_inscripcion'], errors='coerce', format="%d/%m/%Y")
            final_df['processing_time'] = pd.to_datetime(
                final_df['processing_time'], errors='coerce')
            final_df["fecha_fallecimiento"] = pd.to_datetime(
                final_df['fecha_fallecimiento'], errors='coerce')

            final_df.rename(
                columns={
                    "cuit": "PCO_CUIT",
                    "numero_documento": "PCO_NUMERO_DOCUMENTO",
                    "tipo_documento": "PCO_TIPO_DOCUMENTO",
                    "nombre_completo": "PCO_NOMBRE_COMPLETO",
                    "sexo": "PCO_SEXO",
                    "fecha_nacimiento": "PCO_FECHA_NACIMIENTO",
                    "clase": "PCO_CLASE",
                    "localidad": "PCO_LOCALIDAD",
                    "provincia": "PCO_PROVINCIA",
                    "tipo_entidad": "PCO_TIPO_ENTIDAD",
                    "fecha_inscripcion": "PCO_FECHA_INSCRIPCION",
                    "fecha_fallecimiento": "PCO_FECHA_FALLECIMIENTO",
                    "fallecido": "PCO_FALLECIDO",
                    "cuit_baja": "PCO_CUIT_BAJA",
                    "codigo_actividad": "PCO_CODIGO_ACTIVIDAD",
                    "actividad": "PCO_ACTIVIDAD",
                    "anios": "PCO_ANIOS",
                    "anios_inscripcion": "PCO_ANIOS_INSCRIPCION",
                    "jubilado": "PCO_JUBILADO",
                    "pensionado": "PCO_PENSIONADO",
                    "processing_time": "PCO_PROCESSING_TIME",
                    "vista": "PCO_ORIGEN"
                },
                errors='ignore',
                inplace=True
            )
            final_df["PCO_ORIGEN_API"] = "info_experto"

            return final_df
        except Exception as e:
            logger.exception(f"Error extracting identidades {e}")
            raise

    def extract_emails(self, df, processing_time=None) -> None:
        try:
            df_list = []
            for j_row, vista in zip(df[0], df['vista']):
                if j_row:
                    json_data = json.loads(j_row)
                    if json_data.get("estado") == "OK":
                        cuit = json_data["datos"]["identidad"]["cuit"]

                        email_data = []

                        if json_data["datos"]["soaAfipA4Online"]:

                            email_afip = json_data["datos"]["soaAfipA4Online"]["email"]
                            if email_afip:
                                [email_data.append(x["direccion"])
                                    for x in email_afip]

                        email = json_data["datos"]["mails"]
                        if email:
                            [email_data.append(x["mail"]) for x in email]

                        email_df = pd.DataFrame(email_data, columns=["mail"])
                        email_df["cuit"] = cuit
                        email_df["vista"] = vista

                        df_list.append(email_df)

            if len(df_list) == 0:
                logger.warning("No data to extract")
                return pd.DataFrame()

            final_df = pd.concat(df_list, ignore_index=True, axis=0)
            final_df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
            final_df["mail"] = final_df["mail"].str.upper()
            final_df.drop_duplicates(inplace=True)
            final_df["processing_time"] = processing_time

            # refactorizacion data wrangling
            final_df['processing_time'] = pd.to_datetime(
                final_df['processing_time'], errors='coerce')

            final_df.rename(
                columns={
                    "mail": "MCO_MAIL",
                    "cuit": "MCO_CUIT",
                    "vista": "MCO_ORIGEN",
                    "processing_time": "MCO_PROCESSING_TIME",
                },
                errors='ignore',
                inplace=True
            )
            final_df["MCO_ORIGEN_API"] = "info_experto"

            return final_df

        except Exception as e:
            logger.exception(f"Error extracting emails {e}")
            raise

    def extract_domicilios(self, df, processing_time, s3_key=None) -> None:
        try:
            df_list = []

            for j_row, vista in zip(df[0], df['vista']):
                if j_row:
                    json_data = json.loads(j_row)
                    if json_data.get("estado") == "OK":
                        cuit = json_data["datos"]["identidad"]["cuit"]
                        # Datos IE
                        datos_domicilios = json_data["datos"]["domicilios"]
                        if datos_domicilios:
                            ie_data = pd.json_normalize(
                                json_data["datos"]["domicilios"])
                            # o "info_experto"
                            ie_data["tipoOrigen"] = "PrincipalInfoExperto"
                        else:
                            ie_data = pd.DataFrame()

                        # Datos AFIP
                        afipdatos = json_data["datos"]["soaAfipA4Online"]

                        if afipdatos:
                            afipdata = afipdatos["domicilio"]

                            if afipdata:
                                afipdata = pd.json_normalize(
                                    json_data["datos"]["soaAfipA4Online"]["domicilio"])
                                afipdata["tipoOrigen"] = "AFIP"
                                afipdata.rename(
                                    columns={
                                        "descripcionProvincia": "provincia",
                                        "tipoDomicilio": "tipo",
                                        "direccion": "calle",
                                        "codPostal": "cp",
                                    },
                                    inplace=True,
                                )
                            else:
                                afipdata = pd.DataFrame()
                        else:
                            afipdata = pd.DataFrame()

                        ie_dom_df = pd.concat([ie_data, afipdata])

                        requested_cols = [
                            "calle",
                            "altura",
                            "extra",
                            "barrio",
                            "localidad",
                            "provincia",
                            "cp",
                            "tipo",
                            "fecha",
                            "tipoOrigen",
                        ]

                        ie_dom_df = ie_dom_df.reindex(
                            columns=requested_cols, fill_value=np.nan)

                        ie_dom_df["provincia"] = ie_dom_df["provincia"].apply(
                            self.reemplazar_por_provincia
                        )  # OBS: No está mapeando (la columna correcta sería idProvincia) # TODO
                        ie_dom_df["cuit"] = cuit
                        ie_dom_df["vista"] = vista
                        df_list.append(ie_dom_df)

            if len(df_list) == 0:
                logger.warning("No data to extract")
                return pd.DataFrame()

            final_df = pd.concat(df_list, ignore_index=True, axis=0)
            final_df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
            final_df.drop_duplicates(inplace=True)
            final_df["processing_time"] = processing_time

            # refactorización data wrangling
            final_df['fecha'] = pd.to_datetime(
                final_df['fecha'], errors='coerce')
            final_df['processing_time'] = pd.to_datetime(
                final_df['processing_time'], errors='coerce')
            final_df['altura'] = final_df['altura'].replace(0, '0')
            final_df['extra'] = final_df['extra'].replace(0, '0')
            final_df['barrio'] = final_df['barrio'].replace(0, '0')

            final_df.rename(
                columns={
                    "calle": "DCO_CALLE",
                    "altura": "DCO_ALTURA",
                    "extra": "DCO_EXTRA",
                    "barrio": "DCO_BARRIO",
                    "localidad": "DCO_LOCALIDAD",
                    "provincia": "DCO_PROVINCIA",
                    "cp": "DCO_CP",
                    "tipo": "DCO_TIPO",
                    "fecha": "DCO_FECHA",
                    "tipoOrigen": "DCO_ORIGEN_API",
                    "cuit": "DCO_CUIT",
                    "vista": "DCO_ORIGEN",
                    "processing_time": "DCO_PROCESSING_TIME"
                },
                errors='ignore',
                inplace=True
            )
            return final_df
        except Exception as e:
            logger.exception(
                f"Error extracting domicilios {e}. json: {str(j_row)}")
            raise
        
    def reemplazar_por_provincia(self, valor):
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
            24: "TIERRA DEL FUEGO",
        }
        if type(valor) == int:
            return provincia_map.get(valor, valor)
        return valor
    
    def extract_telefonos(self, df, processing_time, s3_key=None) -> None:
        try:
            df_list = []
            for j_row, vista in zip(df[0], df['vista']):
                if j_row:
                    json_data = json.loads(j_row)
                    if json_data.get("estado") == "OK":
                        cuit = json_data["datos"]["identidad"]["cuit"]

                        tel_data = []

                        ie_cel = json_data["datos"]["celulares"]
                        if ie_cel:
                            [tel_data.append(x["numero"]) for x in ie_cel]

                        ie_teldec = json_data["datos"]["telefonosDeclarados"]
                        if ie_teldec:
                            [tel_data.append(x["numero"]) for x in ie_teldec]

                        ie_tel = json_data["datos"]["telefonos"]
                        if ie_tel:
                            [tel_data.append(x["telefono"]) for x in ie_tel]

                        tel_df = pd.DataFrame(tel_data, columns=["numero"])
                        tel_df["cuit"] = cuit
                        tel_df["vista"] = vista

                        df_list.append(tel_df)

            if len(df_list) == 0:
                logger.warning("No data to extract")
                return pd.DataFrame()

            final_df = pd.concat(df_list, ignore_index=True, axis=0)
            final_df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
            final_df.drop_duplicates(inplace=True)
            final_df["processing_time"] = processing_time

            # refactorización data wrangling
            final_df['processing_time'] = pd.to_datetime(
                final_df['processing_time'], errors='coerce')

            final_df.rename(
                columns={
                    "numero": "TCO_NUMERO",
                    "cuit": "TCO_CUIT",
                    "vista": "TCO_ORIGEN",
                    "processing_time": "TCO_PROCESSING_TIME",
                },
                errors='ignore',
                inplace=True
            )
            final_df["TCO_ORIGEN_API"] = "info_experto"

            return final_df

        except Exception as e:
            logger.exception(f"Error extracting identidades {e}")
            raise

    def extract_rodados(self, df, processing_time, s3_key=None) -> None:
        j_row = None
        try:
            df_list = []
            for j_row, vista, ftmfecha, ftmid in zip(df[0], df['vista'], df['ftmfecha'], df['ftmid']):
                if j_row:
                    json_data = json.loads(j_row)
                    if json_data.get("estado") == "OK":
                        cuit = json_data["datos"]["identidad"]["cuit"]

                        rod_data = []

                        rod = json_data["datos"]["rodados"]
                        if rod:
                            [rod_data.append(x) for x in rod]

                        rod_bus = json_data.get(
                            "datos", {}).get("dominioBuscado")
                        if rod_bus:
                            if isinstance(rod_bus, list):
                                [rod_data.append(x) for x in rod_bus]
                            else:
                                rod_data.append(rod_bus)

                        if rod_data:
                            rod_df = pd.DataFrame(rod_data)
                            rod_df["cuit"] = cuit
                            rod_df["vista"] = vista
                            rod_df["ftmfecha"] = ftmfecha
                            rod_df["ftmid"] = ftmid

                            df_list.append(rod_df)

            if len(df_list) == 0:
                logger.warning("No data to extract")
                return pd.DataFrame()

            final_df = pd.concat(df_list, ignore_index=True, axis=0)
            final_df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
            final_df.drop_duplicates(inplace=True)
            final_df["tipo"] = final_df["dominio"].apply(tipo_vehiculo)
            final_df["fecha_transaccion"] = pd.to_datetime(
                final_df["fecha_transaccion"], format="%d/%m/%Y", errors="coerce"
            )
            final_df["tipoOrigen"] = "info_experto"
            final_df["processing_time"] = processing_time
            final_df["source_s3_key"] = s3_key

            # refactorizacion data wrangling
            final_df["modelo"] = final_df["modelo"].astype(
                str).str[0:4].replace('nan', '')
            final_df['fecha_transaccion'] = pd.to_datetime(
                final_df['fecha_transaccion'], errors='coerce')
            final_df['processing_time'] = pd.to_datetime(
                final_df['processing_time'], errors='coerce')
            final_df['ftmfecha'] = pd.to_datetime(
                final_df['ftmfecha'], errors='coerce', unit='s')
            final_df['ftmid'] = final_df['ftmid'].astype(str).str[0:]
            final_df['porcentaje'] = final_df['porcentaje'].astype(
                str).str[:10]

            final_df.rename(
                columns={
                    "dominio": "RCO_DOMINIO",
                    "origen": "RCO_FAB_ORIGEN",
                    "marca": "RCO_MARCA",
                    "version": "RCO_VERSION",
                    "modelo": "RCO_MODELO",
                    "porcentaje": "RCO_PORCENTAJE",
                    "fecha_transaccion": "RCO_FECHA_TRANSACCION",
                    "cuit": "RCO_CUIT",
                    "tipo": "RCO_TIPO_VEHICULO",
                    "cilindrada": "RCO_CILINDRADA1",
                    "tipoOrigen": "RCO_ORIGEN_API",
                    "vista": "RCO_ORIGEN",
                    "processing_time": "RCO_PROCESSING_TIME",
                    "source_s3_key": "BRN_SOURCE_S3_KEY",
                    "ftmfecha": "BRN_FTMFECHA",
                    "ftmid": "BRN_ID_MULTA"
                },
                errors='ignore',
                inplace=True
            )

            return final_df

        except Exception as e:
            logger.exception(f"Error extracting rodados {e}, JSON: {j_row}")
            raise