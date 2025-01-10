def exchange_rate():
    import pandas as pd
    import requests
    from datetime import datetime

    load_dotenv()

    fecha = datetime.now().date()
    API_KEY = os.getenv('ENV_API_KEY')
    BASE_URL = 'http://api.currencylayer.com/'
    URL_LIVE = f'{BASE_URL}live?access_key={API_KEY}'
    URL_HISTORICAL = f'{BASE_URL}historical?access_key={API_KEY}&date={fecha}'

    URL = URL_LIVE

    # Request to API. Extact.
    response = requests.get(URL)
    data = response.json()
    data = data['quotes']

    renamed_data = [{"currency_code": key, "amount": value} for key, value in data.items()]

    # Transform dataframe
    df = pd.DataFrame(renamed_data)
    df['exchange_at'] = datetime.now().date()
    
    return df


def snowflake_load():
    import snowflake.connector

    load_dotenv()
    data = exchange_rate()

    # Config snowflake connection
    sf_account = os.getenv('ENV_SF_ACCOUNT')
    sf_user = os.getenv('ENV_SF_USER')
    sf_password = os.getenv('ENV_SF_PASSWORD')
    sf_warehouse = os.getenv('ENV_SF_WAREHOUSE')
    sf_database = os.getenv('ENV_SF_DATABASE')
    sf_schema = os.getenv('ENV_SF_SCHEMA')

    # Create snowflake connection
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )

    # open transaction
    cursor = conn.cursor()

    print("Inserting data!!")

    # Insert data from each row of the dataframe. Load.
    insert_sql = f"INSERT INTO {sf_database}.{sf_schema}.GLOBAL_EXCHANGE_RATE (currency_code, amount, exchange_at) VALUES (%s, %s, %s)"
    for index, row in data.iterrows():
        cursor.execute(insert_sql, (row['currency_code'], row['amount'], row['exchange_at']))

    # Confirm and close transaction
    conn.commit()
    cursor.close()
    conn.close()

    print("Data successfully inserted.")


if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    snowflake_load()