def exchange_rate():
    import pandas as pd
    import requests
    from datetime import datetime

    load_dotenv()

    country_currency_code = {
        'México': 'MXN',
        'Guatemala': 'GTQ',
        'Belice': 'BZD',
        # 'El Salvador': 'USD',
        'Honduras': 'HNL',
        'Nicaragua': 'NIO',
        'Costa Rica': 'CRC',
        'Panamá': 'PAB',
        'Colombia': 'COP',
        'Venezuela': 'VES',
        # 'Ecuador': 'USD',
        'Perú': 'PEN',
        'Bolivia': 'BOB',
        'Paraguay': 'PYG',
        'Chile': 'CLP',
        'Argentina': 'ARS',
        'Uruguay': 'UYU',
        'Brasil': 'BRL',
        'Cuba': 'CUP',
        'República Dominicana': 'DOP',
        # 'Puerto Rico': 'USD',
        'Haití': 'HTG',
    }

    fecha = '2024-01-01'
    API_KEY = os.getenv('ENV_API_KEY')
    BASE_URL = 'http://api.currencylayer.com/'
    URL_LIVE = f'{BASE_URL}live?access_key={API_KEY}'
    URL_HISTORICAL = f'{BASE_URL}historical?access_key={API_KEY}&date={fecha}'

    URL = URL_LIVE

    # Hacemos la solicitud a la API
    response = requests.get(URL)
    data = response.json()
    data = data['quotes']

    renamed_data = [{"currency_code": key, "amount": value} for key, value in data.items()]

    # Convertir a DataFrame
    df = pd.DataFrame(renamed_data)
    df['exchange_at'] = datetime.now().date()
    
    return df


def snowflake_load():
    import snowflake.connector
    from sqlalchemy import create_engine

    load_dotenv()
    data = exchange_rate()

    # Configuración de conexión a Snowflake
    sf_account = os.getenv('ENV_SF_ACCOUNT')
    sf_user = os.getenv('ENV_SF_USER')
    sf_password = os.getenv('ENV_SF_PASSWORD')
    sf_warehouse = os.getenv('ENV_SF_WAREHOUSE')
    sf_database = os.getenv('ENV_SF_DATABASE')
    sf_schema = os.getenv('ENV_SF_SCHEMA')

    # Crear la conexión con Snowflake
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )

    # Insertar datos en la tabla
    cursor = conn.cursor()

    # Generar el comando INSERT y ejecutarlo por cada fila del DataFrame
    insert_sql = f"INSERT INTO PHYGITAL_PROD.BRONZE_LAYER.GLOBAL_EXCHANGE_RATE (currency_code, amount, exchange_at) VALUES (%s, %s, %s)"
    for index, row in data.iterrows():
        cursor.execute(insert_sql, (row['currency_code'], row['amount'], row['exchange_at']))

    # Confirmar la transacción y cerrar conexión
    conn.commit()
    cursor.close()
    conn.close()

    print("Datos insertados exitosamente en Snowflake.")


if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    snowflake_load()