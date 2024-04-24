import pandas as pd
import boto3
import os
import time
import datetime
import logging
from modules import busquedas
from modules.infoexperto import get_infoexpertos_from_cuit
from modules.data import DataScrapping
from modules.oracle import OracleDataInserter

logger = logging.getLogger()

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
DEV_GLUE_CONN = "oracle_datos_clientes_creds"
DEV_ORACLE_SCHEMA = "DATOS_CLIENTES"
PROD_GLUE_CONN = "oracle_datos_prod_creds"
PROD_ORACLE_SCHEMA = "DATOS_PROD"
BERA_CONEXION = "karstec"
BERA_ESQUEMA = "KARSTEC"
bera_con = OracleDataInserter(schema=BERA_ESQUEMA, glue_con=BERA_CONEXION)
dev_con = OracleDataInserter(schema=DEV_ORACLE_SCHEMA, glue_con=DEV_GLUE_CONN)

listado = dev_con.ejecutar_consulta_sql(
        sql=f"""
            SELECT DISTINCT RESP_PAGO
            FROM RESP_PAGO_KARSTEC
            WHERE RESP_PAGO NOT IN(SELECT NOMBRE||' '||APELLIDO FROM CUIT_ONLINE_PBUS2)
            AND RESP_PAGO NOT IN(SELECT NOMBRE||' '||APELLIDO FROM CUIT_ONLINE_PBUS)
        """)

datos_karstec = listado
columna_resp_pago = ('RESP_PAGO',)

df_listado = pd.DataFrame(datos_karstec, columns=columna_resp_pago)

def get_ssm_parameter(Name, WithDecryption=True):
    try:
        ssm_client = boto3.client("ssm")
        ssm_value = ssm_client.get_parameter(Name=Name, WithDecryption=WithDecryption)
        ssm_value = ssm_value["Parameter"]["Value"]

        return ssm_value

    except Exception as e:
        logger.exception(f"Error getting ssm parameter: {Name}")
        raise

provincia = 'Buenos Aires'

def load_checkpoint():
    try:
        with open('checkpoint.txt', 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 0

def save_checkpoint(lote_num):
    with open('checkpoint.txt', 'w') as f:
        f.write(str(lote_num))

def main():

    start_lote = load_checkpoint()
    # Dividir el DataFrame en lotes de 10 elementos
    num_lotes = len(df_listado) // 10 + (1 if len(df_listado) % 10 > 0 else 0)

    for lote_num in range(start_lote, num_lotes):
        # Obtener el lote actual
        lote_inicio = lote_num * 10
        lote_fin = min((lote_num + 1) * 10, len(df_listado))
        lote_actual = df_listado.iloc[lote_inicio:lote_fin]

        print(f'PRINCIPAL: PROCESANDO LOTE {lote_num + 1} de {num_lotes}')

        # INICIO BUSQUEDAS DE DATOS
        df_primer_paso = busquedas.primer_busqueda_y_guardado(lote_actual)


        df_primer_paso['LOTE'] = lote_num

        if not df_primer_paso.empty:
            dev_con.insert_data(
                df=df_primer_paso,
                table_name="CUIT_ONLINE_PBUS2",
                mode="Append",
            )
            print('LOTE INSERTADO EN DATOS_CLIENTES')

        # Realizar la consulta para obtener datos de CUIT_ONLINE_PBUS
        consulta = dev_con.ejecutar_consulta_sql(
            sql=f"""
                SELECT *
                FROM CUIT_ONLINE_PBUS2
                WHERE CUIT != 'No encontrado'
            """)

        datos_CO = consulta
        columna_resp_pago = ('NOMBRE', 'APELLIDO', 'CUIT', 'LOTE')
        df_resultados = pd.DataFrame(datos_CO, columns=columna_resp_pago)

        df_resultados_filtrados = df_resultados.loc[df_resultados['LOTE'] == lote_num]

        if not df_resultados_filtrados.empty:
        # Realizar la búsqueda final
            datos_finales = busquedas.busqueda_final(df_resultados_filtrados)

        #ELIMINAR DATOS NO NECESARIOS
            quedarse_filas = []
            for index, row in datos_finales.iterrows():
                if len(datos_finales) != 0:
                    if row['provincia'] == provincia:
                        quedarse_filas.append(row)

            final_sprov = pd.DataFrame(quedarse_filas)
            identidades_final_busqueda = final_sprov

            fecha_actual = datetime.date.today()
            identidades_final_busqueda.to_csv('datos_busqueda_ie.csv')

            datos_finales['vista'] = 'CUIT_ONLINE'
            datos_finales['ftmfecha'] = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            datos_finales['ftmid'] = '0'
            #FIN DE BUSQUEDA CUIT ONLINE

            process_infoexperto_and_insert_data(identidades_final_busqueda, datos_finales)

            save_checkpoint(lote_num + 1)


def process_infoexperto_and_insert_data(identidades_final_busqueda, datos_finales):
    #INICIO DE INFOEXPERTO
    info_expertos_api_key = get_ssm_parameter("scrapping-info_expertos_api_key")

    df_f = identidades_final_busqueda 
    logger.info("Consultando IE por CUIT")
    ie_cuit = df_f.apply(
        lambda x: get_infoexpertos_from_cuit(
            x["cuit"], api_key=info_expertos_api_key
        ),
        axis=1,
        result_type="expand",
    )

    ie_cuit = pd.DataFrame(ie_cuit).join(datos_finales['vista'])
    ie_cuit = pd.DataFrame(ie_cuit).join(datos_finales['ftmfecha'])
    ie_cuit = pd.DataFrame(ie_cuit).join(datos_finales['ftmid'])

    data = DataScrapping()
    datetime_now = datetime.datetime.now()
    processing_time = datetime_now.isoformat(timespec="seconds")

    identidades_final = pd.concat([
                        data.extract_identidades(ie_cuit, processing_time)
                        ]).reset_index(drop=True)
    
    domicilios_final = pd.concat([
                        data.extract_domicilios(ie_cuit, processing_time)
                    ]).reset_index(drop=True)
    
    telefonos_final = pd.concat([
                        data.extract_telefonos(ie_cuit, processing_time)
                    ]).reset_index(drop=True)
    
    emails_final = pd.concat([
                        data.extract_emails(ie_cuit, processing_time)
                    ]).reset_index(drop=True)
    
    rodados_final = pd.concat([
                        data.extract_rodados(ie_cuit, processing_time),
                    ]).reset_index(drop=True)

    if not rodados_final.empty: 
        rodados_final = rodados_final.drop(['BRN_SOURCE_S3_KEY', 'BRN_FTMFECHA', 'BRN_ID_MULTA'], axis=1)

    if not identidades_final.empty:
                    dev_con.insert_data(
                        df=identidades_final,
                        table_name="BERA_PERSONAS_CO",
                        mode="Append",
                    ) 
                    print('PERSONAS INSERTADAS EN ORACLE')

    if not domicilios_final.empty:
                    dev_con.insert_data(
                        df=domicilios_final,
                        table_name="BERA_DOMICILIOS_CO",
                        mode="Append",
                    )
                    print('DOMICILIOS INSERTADOS EN ORACLE') 

    if not telefonos_final.empty:
                    dev_con.insert_data(
                        df=telefonos_final,
                        table_name="BERA_TELS_CO",
                        mode="Append",
                    )
                    print('TELEFONOS INSERTADOS EN ORACLE')

    if not emails_final.empty:
                    dev_con.insert_data(
                        df=emails_final,
                        table_name="BERA_MAILS_CO",
                        mode="Append",
                    )
                    print('MAILS INSERTADOS EN ORACLE')

    if not rodados_final.empty:
                    dev_con.insert_data(
                        df=rodados_final,
                        table_name="BERA_RODADOS_CO",
                        mode="Append",
                    )
                    print('RODADOS INSERTADOS EN ORACLE')

  
if __name__ == '__main__':
    main()