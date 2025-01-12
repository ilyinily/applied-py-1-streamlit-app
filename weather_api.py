import asyncio

import pandas as pd
import requests
import time
from data_generation import month_to_season

api_key = "here I had my own API key"

# Узнаем, какое сейчас время года
current_season = month_to_season[time.localtime().tm_mon]

# Определяем координаты нужного города (вспомогательное упражнение)

needed_city = {'name': "London",
               'current_season': current_season}

res = requests.get(url="http://api.openweathermap.org/geo/1.0/direct",
                   params={'q': needed_city['name'],
                           'appid': api_key,
                           'limit': 5})

print(res.json())

needed_city['lat'] = res.json()[0]['lat']
needed_city['lon'] = res.json()[0]['lon']

# Теперь можно и погоду узнать

# res = requests.get(url="https://api.openweathermap.org/data/2.5/weather",
#                    params={'lat': needed_city['lat'],
#                            'lon': needed_city['lon'],
#                            'units': "metric",
#                            'appid': api_key}
#                    )

# Мы заранее подготовили вспомогательные данные по типичной погоде в это время года, воспользуемся же ими!
df_temp_helper = pd.read_csv("./additional_values.csv")

needed_city['temp_range_min'] = round(float(df_temp_helper[(df_temp_helper['city'] == needed_city['name']) & (
            df_temp_helper['season'] == needed_city['current_season'])]['mean_minus_std'].iloc[0]), 2)
needed_city['temp_range_max'] = round(float(df_temp_helper[(df_temp_helper['city'] == needed_city['name']) & (
            df_temp_helper['season'] == needed_city['current_season'])]['mean_plus_std'].iloc[0]), 2)



# Полагаю, что для получения текущей температуры лучше использовать асинхронные методы, потому что трудно сказать, когда там нам внешний источник ответит
# Но для целей этого эксперимента добыча этой информации всё равно нужна, прежде чем продолжать, поэтому существенной разницы нет


async def get_weather_async(url="https://api.openweathermap.org/data/2.5/weather"):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {'lat': needed_city['lat'],
              'lon': needed_city['lon'],
              'units': "metric",
              'appid': api_key}
    result = await asyncio.to_thread(requests.get,
                                     url=url,
                                     params=params
                                     )
    return result

async_res = asyncio.run(get_weather_async())

needed_city['current_temp'] = async_res.json()['main']['temp']
needed_city['temp_anomaly'] = "Expected temperature" if needed_city['temp_range_min'] <= needed_city['current_temp'] <= \
                                                        needed_city['temp_range_max'] else "Anomaly"


print(needed_city)