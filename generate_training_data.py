import polars as pl
import json
from typing import List, Dict
import random
from datetime import datetime, timedelta

def generate_historical_training_data() -> List[Dict]:
    """Генерация синтетических данных для обучения ML модели"""
    print("🤖 Генерация тренировочных данных для ML...")
    
    training_data = []
    
    # Генерируем разнообразные примеры
    user_profiles = [
        {'spending_level': 'low', 'interaction_frequency': 'low', 'total_spent': 5000},
        {'spending_level': 'medium', 'interaction_frequency': 'medium', 'total_spent': 25000},
        {'spending_level': 'high', 'interaction_frequency': 'high', 'total_spent': 80000},
        {'spending_level': 'very_high', 'interaction_frequency': 'very_high', 'total_spent': 200000},
    ]
    
    products = [
        {'id': 'premium_card_1', 'business_value': 0.9, 'type': 'premium_cards'},
        {'id': 'credit_1', 'business_value': 0.8, 'type': 'credit_cards'},
        {'id': 'savings_1', 'business_value': 0.7, 'type': 'savings'},
        {'id': 'investment_1', 'business_value': 0.85, 'type': 'investment'},
    ]
    
    for user in user_profiles:
        for product in products:
            # Имитируем разную конверсию в зависимости от профиля и продукта
            if user['spending_level'] in ['high', 'very_high'] and product['type'] in ['premium_cards', 'investment']:
                conversion_rate = random.uniform(0.6, 0.9)
            elif user['spending_level'] == 'medium' and product['type'] in ['credit_cards', 'savings']:
                conversion_rate = random.uniform(0.4, 0.7)
            else:
                conversion_rate = random.uniform(0.1, 0.4)
            
            training_data.append({
                'user_profile': user,
                'product': product,
                'conversion_rate': conversion_rate,
                'converted': conversion_rate > 0.5,  # Бинарная конверсия
                'llm_insights': {
                    'price_segment': user['spending_level'],
                    'spending_impact': 'high' if user['spending_level'] in ['high', 'very_high'] else 'medium'
                }
            })
    
    print(f"✅ Сгенерировано {len(training_data)} тренировочных примеров")
    return training_data

def save_training_data(data: List[Dict], filename: str = "training_data.json"):
    """Сохранение тренировочных данных"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Тренировочные данные сохранены в {filename}")

if __name__ == "__main__":
    training_data = generate_historical_training_data()
    save_training_data(training_data)