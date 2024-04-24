import logging

import requests

logger = logging.getLogger()

def get_infoexpertos_from_cuit(cuit, api_key) -> str:
    cuit = str(cuit).replace('-', '').strip()
    base_url = "https://servicio.infoexperto.com.ar/app/api/v1/informe/apikey"
    url = f"{base_url}/{api_key}/cuit/{cuit}"
    r = requests.get(url)
    r.raise_for_status()
    data_texto = r.text
    return data_texto