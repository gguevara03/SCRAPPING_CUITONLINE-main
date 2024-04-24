from datetime import datetime

def es_numero(p_valor):
    try:
        float(p_valor)
        return 'S'
    except ValueError:
        return 'N'


def tipoVehiculo(dominio):
    if es_numero(dominio[:3]) == 'S' or es_numero(dominio[1:4]) == 'S':
        return 'M'
    else:
        return 'A'

def fecha_actual_MMDDYYYY():
    return datetime.now().strftime('%m%d%Y')