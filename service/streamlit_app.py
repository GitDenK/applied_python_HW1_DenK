import streamlit as st
import pandas as pd
import plotly.express as px
import time
import asyncio
from data_analysis import parallel_analysis, analyze_city
from weather_api import get_current_weather_sync, get_current_weather_async

def main():
    st.title('Анализ температуры')

    st.header('Исторические данныe')

    data = None
    # Выбор источника данных
    data_source = st.radio('Выберите источник данных:', ('Использовать дефолтные данные', 'Загрузить файл'))
    if data_source == 'Загрузить файл':
        uploaded_file = st.file_uploader("Загрузите файл в формате CSV", type="csv")
        if uploaded_file:
            try:
                data = pd.read_csv(uploaded_file)
            except Exception as e:
                st.error(f"Ошибка при чтении файла: {e}")
                return
    else:
        data = pd.read_csv('temperature_data.csv')

    if data is not None:
        # Параллельное или последовательное выполнение
        analysis_type = st.radio('Метод анализа:', ['Параллельно', 'Последовательно'])
        chosen_cities = st.multiselect('Выберите города для анализа:', data['city'].unique(), default=data['city'].unique())
        threshold = st.slider('Порог аномалии (кол-во средних отклонений)', min_value=0.1, max_value=5.0, value=2.0, step=0.01, key=1)

        if st.button('Начать анализ'):
            with st.spinner('Анализ данных...'):
                start_time = time.time()
                if analysis_type == 'Параллельно':
                    analysis_results = parallel_analysis(data, threshold)
                else:
                    analysis_results = [analyze_city(group, threshold) for _, group in data.groupby('city')]
                end_time = time.time()

                elapsed_time = time.time() - start_time
                st.write(f"Время на расчеты: {elapsed_time:.2f} секунд")

                for city_data, seasonal_stats in analysis_results:
                    city_name = city_data['city'].iloc[0]
                    if city_name not in chosen_cities:
                        continue
                    st.subheader(f"{city_name}")
                    
                    # Ограничение данных так как иначе ломается график
                    latest_data = city_data.tail(1000)
                    latest_anomalies = latest_data[latest_data['anomaly']]
                    
                    st.dataframe(latest_data)
                    st.write("Сезонная статистика:")
                    st.dataframe(seasonal_stats)

                    # Создаем график
                    fig = px.line(latest_data, x='timestamp', y='temperature', title=f'Температура с аномалиями в {city_name}')
                    fig.add_scatter(x=latest_data['timestamp'], y=latest_data['moving_average'],
                                    mode='lines', line=dict(color='green', width=2), name='Скользящее среднее')
                    fig.add_scatter(x=latest_anomalies['timestamp'], y=latest_anomalies['temperature'],
                                    mode='markers', marker=dict(color='red'), name='Аномалии')
                    st.plotly_chart(fig)

                    st.write(f"Количество аномальных значений: {latest_anomalies.shape[0]}")


        st.header('Текущая температура')
        api_key = st.text_input('Введите API ключ OpenWeatherMap:')

        async def analyze_and_fetch_weather(city, api_key, city_data):
            """анализ текущей температуры асинхронно"""
            weather_task = asyncio.create_task(get_current_weather_async(city, api_key))
            # Параллельный расчет сезонной статистики
            city_data, seasonal_stats = analyze_city(data[data['city'] == city])
            season_stats = seasonal_stats[seasonal_stats['season'] == current_season]
            # Ожидание завершения API запроса
            weather_data = await weather_task
            return weather_data, seasonal_stats

        if api_key:
            city = st.selectbox('Выберите город для мониторинга:', options=data['city'].unique())

            threshold_now = st.slider('Порог аномалии (кол-во средних отклонений)', min_value=0.1, max_value=5.0, value=2.0, step=0.01, key=2)

            # Синхронный или асинхронный вызов API
            request_type = st.radio('Способ запроса к API:', ['Синхронный', 'Асинхронный'])
            if st.button('Получить текущую температуру'):

                # Определение текущего сезона
                month_to_season = {12: "winter", 1: "winter", 2: "winter",
                    3: "spring", 4: "spring", 5: "spring",
                    6: "summer", 7: "summer", 8: "summer",
                    9: "autumn", 10: "autumn", 11: "autumn"}
                current_season = month_to_season[time.localtime().tm_mon]

                if request_type == 'Синхронный':
                    start_time = time.time()
                    # запрос к апи
                    weather_data = get_current_weather_sync(city, api_key)
                    #  вычисление температуры 
                    city_data, seasonal_stats = analyze_city(data[data['city'] == city])
                    season_stats = seasonal_stats[seasonal_stats['season'] == current_season]
                    elapsed_time = time.time() - start_time
                    st.write(f"Время на расчеты: {elapsed_time:.2f} секунд")
                else:
                    start_time = time.time()
                    weather_data, seasonal_stats = asyncio.run(analyze_and_fetch_weather(city, api_key, data))
                    season_stats = seasonal_stats[seasonal_stats['season'] == current_season]
                    elapsed_time = time.time() - start_time
                    st.write(f"Время на расчеты: {elapsed_time:.2f} секунд")

                if 'error' in weather_data:
                    st.error(weather_data['error'])
                else:
                    current_temperature = weather_data['main']['temp']

                    if not season_stats.empty:
                        mean_temp = season_stats['mean'].values[0]
                        std_temp = season_stats['std'].values[0]
                        if current_temperature < mean_temp - threshold_now * std_temp or current_temperature > mean_temp + threshold_now * std_temp:
                            st.warning(f"Температура {current_temperature} аномальная для сезона {current_season}. Обычно {mean_temp:.2f} градусов")
                        else:
                            st.success(f"Температура {current_temperature} нормальная для сезона {current_season}. Обычно {mean_temp:.2f} градусов")
                    else:
                        st.error(f"Не удалось определить статистику для сезона {current_season}.")

main()
