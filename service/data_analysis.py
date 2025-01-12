import pandas as pd
import numpy as np
from multiprocessing import Pool

def calculate_moving_statistics(data, window_size=30):
    """Вычисляет скользящее среднее и стандартное отклонение с заданным размером окна"""
    rolling_window = data['temperature'].rolling(window=window_size)
    moving_avg = rolling_window.mean()
    moving_std = rolling_window.std()
    return moving_avg, moving_std

def detect_anomalies_with_moving_stats(data, threshold=2):
    """Определяет аномали температуры на основе скользящего среднего и стандартного отклонения"""
    data['anomaly'] = ((data['temperature'] < data['moving_average'] - threshold * data['moving_std']) |
                       (data['temperature'] > data['moving_average'] + threshold * data['moving_std']))
    return data

def calculate_seasonal_stats(data):
    """Рассчитывает среднюю температуру и стандартное отклонение для каждого сезона"""
    seasonal_stats = data.groupby('season')['temperature'].agg(['mean', 'std']).reset_index()
    return seasonal_stats

def analyze_city(data, threshold=2):
    """Выполняет полный анализ данных для одного города"""
    moving_avg, moving_std = calculate_moving_statistics(data)
    data['moving_average'] = moving_avg
    data['moving_std'] = moving_std
    data = detect_anomalies_with_moving_stats(data, threshold)
    seasonal_stats = calculate_seasonal_stats(data)
    return data, seasonal_stats

def analyze_city_wrapper(args):
    return analyze_city(*args)

def parallel_analysis(data, threshold):
    """Запускает анализ данных параллельно по городам"""
    with Pool() as pool:
        results = pool.map(analyze_city_wrapper, [(city_data, threshold) for city, city_data in data.groupby('city')])
    return results
