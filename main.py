#!/usr/bin/env python3
"""
Crypto Indices Data Collection Script
Автоматический сбор данных с LunarCrush API и сохранение в Supabase
"""

import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from typing import Dict, List, Optional, Tuple
import warnings
from supabase import create_client, Client
import psycopg2
from psycopg2.extras import Json
import logging

warnings.filterwarnings('ignore')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('crypto_indices.log')
    ]
)

logger = logging.getLogger(__name__)

# Конфигурация API и базы данных из переменных окружения
def get_env_variable(name: str, required: bool = True) -> Optional[str]:
    """Получает переменную окружения с обработкой ошибок"""
    value = os.getenv(name)
    if required and not value:
        logger.error(f"Обязательная переменная окружения {name} не установлена")
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value

# API конфигурация
API_URL = "https://lunarcrush.com/api4/public/coins/list/v1"
API_KEY = get_env_variable('LUNARCRUSH_API_KEY')

# Supabase конфигурация
SUPABASE_URL = get_env_variable('SUPABASE_URL')
SUPABASE_KEY = get_env_variable('SUPABASE_KEY')

# PostgreSQL конфигурация
POSTGRES_CONFIG = {
    "user": get_env_variable('POSTGRES_USER'),
    "password": get_env_variable('POSTGRES_PASSWORD'),
    "host": get_env_variable('POSTGRES_HOST'),
    "port": int(get_env_variable('POSTGRES_PORT', False) or 5432),
    "database": get_env_variable('POSTGRES_DATABASE')
}

# Инициализация клиента Supabase
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase клиент успешно инициализирован")
except Exception as e:
    logger.error(f"Ошибка инициализации Supabase клиента: {e}")
    raise

# КОНФИГУРАЦИЯ ИНДЕКСОВ
INDICES = {
    "RWA": {
        "name": "Real World Assets",
        "description": "Токены, связанные с реальными активами",
        "tokens": {
            46356: "ONDO",   # Ondo Finance
            158332: "SYRUP", # Maple Finance
            18: "LINK",      # Chainlink
            159642: "PLUME", # PLUME
            158860: "USUAL", # USUAL
            149893: "ENA",   # Ethena
            2141: "XAUT",    # Tether Gold
            14846: "CFG",    # Centrifuge
            157749: "USTBL", # Spiko
            7035: "DAO",     # DAO Maker
            20358: "CPOOL",  # Clearpool
            17: "IOTA",      # IOTA
            70319: "SKY",    # Sky
            51211: "CTC",    # Creditcoin
            12112: "EPIC",   # Epic Chain
            279: "RSR",      # Reserve Rights
            158179: "USYC",  # Circle USYC
            160101: "CUSDO", # OpenDollar
            431: "TRAC",     # OriginTrail
            30504: "POLYX",  # Polymesh
            51116: "CHEX",   # Chintai
            7861: "WHITE",   # WhiteRock
            2868: "VSN",     # Vision
            152191: "ZBCN",  # Zebec Network
            159759: "COLLAT",# collaterize
            151919: "PRCL",  # Parcl
            160799: "HUMA"   # Huma Finance
        }
    },

    "DEFI": {
        "name": "DeFi",
        "description": "Протоколы децентрализованных финансов",
        "tokens": {
            28: "UNI",       # Uniswap
            1048: "AAVE",    # Aave
            7035: "MKR",     # MakerDAO
            3094: "COMP",    # Compound
            1844: "CRV",     # Curve
            1758: "SUSHI",   # SushiSwap
            7040: "YFI",     # Yearn Finance
            261: "SNX",      # Synthetix
            1834: "1INCH",   # 1inch
            33193: "RUNE",   # THORChain
            2477: "BAL",     # Balancer
            30208: "LDO",    # Lido DAO
            52713: "JOE",    # TraderJoe
            154003: "JUP"    # Jupiter
        }
    },

    "MEME": {
        "name": "Meme Coins",
        "description": "Мем-токены и community-driven проекты",
        "tokens": {
            12: "DOGE",      # Dogecoin
            285: "SHIB",     # Shiba Inu
            159246: "PEPE",  # Pepe
            70317: "FLOKI",  # Floki
            70056: "WIF",    # dogwifhat
            156269: "BONK",  # Bonk
            159989: "BRETT", # Brett
            160097: "MEW",   # cat in a dogs world
            157068: "POPCAT",# Popcat
            160321: "MOG",   # Mog Coin
            160636: "TURBO", # Turbo
            160833: "NEIRO"  # Neiro
        }
    }
}

# Единые веса для всех индексов
WEIGHTS = {
    'sentiment': 0.35,           # 35% вес
    'interactions_24h': 0.15,    # 25% вес
    'social_volume_24h': 0.15,   # 25% вес
    'alt_rank': 0.35             # 35% вес
}

# Базовые значения для сигмоидной нормализации
NORMALIZATION_BASELINES = {
    'interactions_24h': 500000,    # Медианное значение для interactions
    'social_volume_24h': 10000,    # Медианное значение для social volume
    'alt_rank_divisor': 100        # Делитель для alt_rank
}


def normalize_sentiment(sentiment: float) -> float:
    """Нормализует sentiment (уже в диапазоне 0-100) в диапазон 0-1"""
    return sentiment / 100 if sentiment else 0


def normalize_alt_rank(alt_rank: float) -> float:
    """
    Нормализует alt_rank используя обратную экспоненциальную функцию.
    Чем меньше ранг, тем лучше (1 = лучший).
    """
    if alt_rank <= 0:
        return 0
    divisor = NORMALIZATION_BASELINES['alt_rank_divisor']
    return 1 / (1 + alt_rank / divisor)


def normalize_interactions(interactions: float) -> float:
    """Нормализует interactions используя сигмоидную функцию."""
    if interactions < 0:
        return 0
    baseline = NORMALIZATION_BASELINES['interactions_24h']
    return interactions / (interactions + baseline)


def normalize_social_volume(volume: float) -> float:
    """Нормализует social_volume используя сигмоидную функцию."""
    if volume < 0:
        return 0
    baseline = NORMALIZATION_BASELINES['social_volume_24h']
    return volume / (volume + baseline)


def calculate_weighted_index(metrics: Dict[str, float]) -> float:
    """Рассчитывает взвешенный индекс от 1 до 10"""
    weighted_sum = sum(metrics[key] * WEIGHTS[key] for key in WEIGHTS.keys())
    return 1 + (weighted_sum * 9)


def validate_normalized_metrics(metrics: Dict[str, float], index_name: str) -> bool:
    """Проверяет, что все нормализованные метрики в диапазоне 0-1"""
    valid = True
    for key, value in metrics.items():
        if value < 0 or value > 1:
            logger.warning(f"[{index_name}] Метрика {key} вне диапазона [0,1]: {value}")
            valid = False
    return valid


def fetch_market_data() -> Optional[List[Dict]]:
    """Получает данные с API LunarCrush"""
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }

    try:
        logger.info("Отправка запроса к API...")
        response = requests.get(API_URL, headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()

        if 'data' in data:
            logger.info(f"Получено {len(data['data'])} токенов")
            return data['data']
        else:
            logger.error("Неожиданная структура ответа API")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к API: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON: {e}")
        return None


def filter_index_tokens(market_data: List[Dict], index_tokens: Dict[int, str]) -> Dict[int, Dict]:
    """Фильтрует данные по токенам конкретного индекса"""
    filtered_data = {}
    missing_tokens = []

    for token_id, token_symbol in index_tokens.items():
        token_found = False

        for token_data in market_data:
            if token_data.get('id') == token_id:
                token_found = True

                # Извлекаем нужные метрики
                metrics = {}
                required_fields = ['interactions_24h', 'social_volume_24h', 'sentiment', 'alt_rank']
                missing_fields = []

                for field in required_fields:
                    if field in token_data and token_data[field] is not None:
                        metrics[field] = float(token_data[field])
                    else:
                        missing_fields.append(field)
                        metrics[field] = 0

                # Добавляем дополнительную информацию
                metrics['symbol'] = token_data.get('symbol', token_symbol)
                metrics['name'] = token_data.get('name', '')
                metrics['price'] = token_data.get('price', 0)
                metrics['market_cap'] = token_data.get('market_cap', 0)

                if missing_fields:
                    logger.warning(f"{token_symbol}: отсутствуют поля: {', '.join(missing_fields)}")

                filtered_data[token_id] = metrics
                break

        if not token_found:
            missing_tokens.append(token_symbol)

    if missing_tokens:
        logger.warning(f"Токены не найдены: {', '.join(missing_tokens)}")

    return filtered_data


def calculate_single_index(filtered_tokens: Dict[int, Dict], index_code: str, index_info: Dict) -> Optional[Dict]:
    """Рассчитывает индекс для одного сектора"""
    if not filtered_tokens:
        logger.error(f"[{index_code}] Нет данных для расчета индекса")
        return None

    # Собираем все значения для агрегации
    all_interactions = [t['interactions_24h'] for t in filtered_tokens.values()]
    all_volumes = [t['social_volume_24h'] for t in filtered_tokens.values()]
    all_sentiments = [t['sentiment'] for t in filtered_tokens.values()]
    all_alt_ranks = [t['alt_rank'] for t in filtered_tokens.values() if t['alt_rank'] > 0]

    # Суммарные/средние значения (до нормализации)
    total_raw = {
        'interactions_24h': sum(all_interactions),
        'social_volume_24h': sum(all_volumes),
        'sentiment': np.mean([s for s in all_sentiments if s > 0]) if any(all_sentiments) else 0,
        'alt_rank': np.mean(all_alt_ranks) if all_alt_ranks else 500
    }

    # Нормализация метрик
    normalized_metrics = {
        'interactions_24h': normalize_interactions(total_raw['interactions_24h']),
        'social_volume_24h': normalize_social_volume(total_raw['social_volume_24h']),
        'sentiment': normalize_sentiment(total_raw['sentiment']),
        'alt_rank': normalize_alt_rank(total_raw['alt_rank'])
    }

    # Валидация
    validate_normalized_metrics(normalized_metrics, index_code)

    # Рассчитываем финальный индекс
    index_value = calculate_weighted_index(normalized_metrics)

    # Подготавливаем результат
    result = {
        'index_code': index_code,
        'index_name': index_info['name'],
        'description': index_info['description'],
        'timestamp': datetime.now().isoformat(),
        'index_value': round(index_value, 2),
        'tokens_count': len(filtered_tokens),
        'raw_metrics': total_raw,
        'normalized_metrics': {k: round(v, 4) for k, v in normalized_metrics.items()},
        'weights': WEIGHTS,
        'tokens': {}
    }

    # Добавляем информацию по каждому токену
    for token_id, data in filtered_tokens.items():
        result['tokens'][data['symbol']] = {
            'id': token_id,
            'name': data['name'],
            'price': round(data['price'], 2) if data['price'] else 0,
            'sentiment': data['sentiment'],
            'alt_rank': data['alt_rank'],
            'interactions_24h': data['interactions_24h'],
            'social_volume_24h': data['social_volume_24h']
        }

    return result


def calculate_all_indices(market_data: List[Dict]) -> Dict[str, Dict]:
    """Рассчитывает все сконфигурированные индексы"""
    all_results = {}

    logger.info("Начало расчета секторных индексов")

    for index_code, index_info in INDICES.items():
        logger.info(f"Обработка индекса: {index_info['name']} ({len(index_info['tokens'])} токенов)")

        # Фильтруем токены для этого индекса
        filtered_tokens = filter_index_tokens(market_data, index_info['tokens'])

        if filtered_tokens:
            # Рассчитываем индекс
            index_result = calculate_single_index(filtered_tokens, index_code, index_info)

            if index_result:
                all_results[index_code] = index_result
                logger.info(f"Индекс {index_code} рассчитан: {index_result['index_value']}/10 ({len(filtered_tokens)}/{len(index_info['tokens'])} токенов)")
        else:
            logger.error(f"Не удалось получить данные для индекса {index_code}")

    return all_results


def save_to_supabase(result: Dict) -> bool:
    """Сохраняет результат расчета индекса в Supabase"""
    try:
        # Подготовка данных для вставки
        data = {
            'index_code': result['index_code'],
            'index_name': result['index_name'],
            'index_value': float(result['index_value']),
            'tokens_count': result['tokens_count'],
            'raw_metrics': result['raw_metrics'],
            'normalized_metrics': result['normalized_metrics'],
            'weights': result['weights'],
            'tokens': result['tokens'],
            'timestamp': result['timestamp']
        }

        # Вставка данных в таблицу
        response = supabase.table('index_metrics').insert(data).execute()

        if response.data:
            logger.info(f"Данные для {result['index_code']} успешно сохранены в Supabase")
            return True
        else:
            logger.error(f"Ошибка при сохранении {result['index_code']} в Supabase")
            return False

    except Exception as e:
        logger.error(f"Ошибка при сохранении в Supabase: {e}")
        return False


def save_all_indices_to_supabase(results: Dict[str, Dict]) -> bool:
    """Сохраняет результаты всех индексов в Supabase"""
    success_count = 0

    logger.info("Сохранение результатов в Supabase...")

    for index_code, result in results.items():
        if save_to_supabase(result):
            success_count += 1

    logger.info(f"Сохранено индексов: {success_count}/{len(results)}")
    return success_count == len(results)


def clean_old_records(days_to_keep: int = 7):
    """Удаляет старые записи из базы данных"""
    try:
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()

        response = supabase.table('index_metrics')\
            .delete()\
            .lt('timestamp', cutoff_date)\
            .execute()

        deleted_count = len(response.data) if response.data else 0
        logger.info(f"Удалено старых записей: {deleted_count}")

    except Exception as e:
        logger.error(f"Ошибка при очистке старых записей: {e}")


def test_supabase_connection() -> bool:
    """Тестирует подключение к Supabase"""
    try:
        response = supabase.table('index_metrics').select('*').limit(1).execute()
        logger.info("Подключение к Supabase успешно установлено")
        return True
    except Exception as e:
        logger.error(f"Ошибка подключения к Supabase: {e}")
        return False


def run_multi_index_calculation():
    """Полный цикл расчета всех индексов с сохранением в Supabase"""
    logger.info(f"Запуск мультииндексного калькулятора - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Тестируем подключение к Supabase
    if not test_supabase_connection():
        logger.error("Проблемы с подключением к Supabase")
        return None

    # Получаем данные с API
    market_data = fetch_market_data()
    if not market_data:
        logger.error("Не удалось получить данные с API")
        return None

    # Рассчитываем все индексы
    all_results = calculate_all_indices(market_data)

    if not all_results:
        logger.error("Не удалось рассчитать ни одного индекса")
        return None

    # Сохраняем результаты в Supabase
    if save_all_indices_to_supabase(all_results):
        logger.info("Все данные успешно сохранены")
    else:
        logger.warning("Не все данные были сохранены")

    # Опционально: очистка старых записей
    clean_old_records(days_to_keep=7)

    # Выводим сводку
    for index_code, result in all_results.items():
        logger.info(f"{result['index_name']}: {result['index_value']}/10 ({result['tokens_count']} токенов)")

    logger.info(f"Расчет завершен успешно. Обработано индексов: {len(all_results)}")
    return all_results


def main():
    """Главная функция"""
    try:
        logger.info("🚀 Запуск системы сбора криптовалютных индексов")
        
        # Проверяем все необходимые переменные окружения
        required_vars = [
            'LUNARCRUSH_API_KEY', 'SUPABASE_URL', 'SUPABASE_KEY',
            'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_HOST', 'POSTGRES_DATABASE'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise EnvironmentError(f"Отсутствуют переменные окружения: {', '.join(missing_vars)}")

        # Запуск основной логики
        results = run_multi_index_calculation()
        
        if results:
            logger.info("✅ Задача выполнена успешно")
            return 0
        else:
            logger.error("❌ Задача завершилась с ошибками")
            return 1
            
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
