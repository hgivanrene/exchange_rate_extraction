def exchange_rate():
    import os
    import requests
    from dotenv import load_dotenv

    load_dotenv()

    API_KEY = os.getenv('ENV_API_KEY')
    URL = f'http://api.currencylayer.com/live?access_key={API_KEY}'

    # Hacemos la solicitud a la API
    response = requests.get(URL)
    data = response.json()
    print(data)

    # # Verificamos si la solicitud fue exitosa
    # if data['success']:
    #     # Obtenemos las tasas de cambio
    #     rates = data['quotes']

    #     # Monedas de LATAM para filtrar
    #     monedas_latam = ['USD', 'ARS', 'BRL', 'CLP', 'COP', 'MXN', 'PEN', 'PYG', 'UYU', 'VES']

    #     # Imprimimos las tasas de cambio para las monedas de LATAM
    #     for moneda in monedas_latam:
    #         key = f'USD{moneda}'
    #         if key in rates:
    #             print(f'Tasa de cambio USD a {moneda}: {rates[key]}')
    #         else:
    #             print(f'No hay datos para la moneda {moneda}')
    # else:
    #     print(f'Error en la solicitud: {data["error"]["info"]}')


if __name__ == '__main__':
    exchange_rate()