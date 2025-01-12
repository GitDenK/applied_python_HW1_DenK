import requests
import asyncio
import aiohttp

API_URL = "http://api.openweathermap.org/data/2.5/weather"

def get_current_weather_sync(city, api_key):
    """Синхронный запрос к апи"""
    try:
        response = requests.get(API_URL, params={'q': city, 'appid': api_key,'units': 'metric'})
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as http_err:
        return {'error': str(http_err)}

async def get_current_weather_async(city, api_key):
    """Асинхронный запрос к апи"""
    async with aiohttp.ClientSession() as session:
        async with session.get(API_URL, params={'q': city, 'appid': api_key,'units': 'metric'}) as response:
            return await response.json()

