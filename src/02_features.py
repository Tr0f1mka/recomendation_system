# src/02_features_enhanced.py
import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# Расширенный список продуктов банка
BANK_PRODUCTS_ENHANCED = {
    'consumer_loan': 'Потребительский кредит наличными',
    'refinancing': 'Рефинансирование кредитов',
    'mortgage': 'Ипотека',
    'savings_account': 'Сберегательный счет',
    'deposit_profitable': 'Вклад "ПСБ.Выгодный"',
    'premium_card': 'Премиальная карта',
    'credit_card_180': 'Кредитная карта "180 дней без %"',
    'salary_card': 'Зарплатная карта',
    'sports_card': 'Карта "Только вперёд"',
    'pension_card': 'Пенсионная карта'
}

def load_sample_users(events_path, max_users=5000):
    """Загружаем пользователей из файлов"""
    users = set()
    files = os.listdir(events_path)[:3]  # 3 файла
    
    for file in tqdm(files, desc="Загрузка пользователей"):
        try:
            df = pd.read_parquet(f'{events_path}/{file}')
            if 'user_id' in df.columns:
                users.update(df['user_id'].unique())
        except Exception as e:
            print(f"⚠️ Ошибка загрузки {file}: {e}")
    
    return list(users)[:max_users]

def create_enhanced_features():
    """Создаем фичи с расширенными продуктами"""
    print("🎯 СОЗДАЕМ ФИЧИ С 10 КАТЕГОРИЯМИ ПРОДУКТОВ...")
    
    base_path = 'data/dataset/small'
    
    # Загружаем пользователей
    market_users = load_sample_users(f'{base_path}/marketplace/events')
    features_data = []
    
    for user_id in tqdm(market_users[:2000], desc="Расширенные фичи"):
        try:
            user_features = {'user_id': user_id}
            
            # Marketplace фичи
            market_features = get_marketplace_features_enhanced(user_id, base_path)
            user_features.update(market_features)
            
            # Offers фичи
            offers_features = get_offers_features_enhanced(user_id, base_path)
            user_features.update(offers_features)
            
            # Retail фичи
            retail_features = get_retail_features_enhanced(user_id, base_path)
            user_features.update(retail_features)
            
            features_data.append(user_features)
            
        except Exception as e:
            continue
    
    features_df = pd.DataFrame(features_data)
    
    # Создаем расширенную целевую переменную
    features_df = create_enhanced_target(features_df)
    
    # Сохраняем
    features_df.to_parquet('user_features_enhanced.pq', index=False)
    
    print(f"💾 Сохранено {len(features_df)} пользователей с 10 категориями")
    print(f"📊 Распределение:")
    print(features_df['target_product'].value_counts())
    
    return features_df

def get_marketplace_features_enhanced(user_id, base_path):
    """Расширенные фичи из marketplace"""
    features = {}
    try:
        events_path = f'{base_path}/marketplace/events'
        files = os.listdir(events_path)[:2]
        
        all_user_events = []
        for file in files:
            df = pd.read_parquet(f'{events_path}/{file}')
            user_events = df[df['user_id'] == user_id]
            all_user_events.append(user_events)
        
        if all_user_events:
            user_data = pd.concat(all_user_events, ignore_index=True)
            
            # Базовые фичи
            features['market_events'] = len(user_data)
            features['market_unique_items'] = user_data['item_id'].nunique()
            
            # Действия
            action_counts = user_data['action_type'].value_counts()
            features['market_views'] = action_counts.get('view', 0)
            features['market_clicks'] = action_counts.get('click', 0) + action_counts.get('clickout', 0)
            features['market_likes'] = action_counts.get('like', 0)
            
            # Поддомены
            subdomain_counts = user_data['subdomain'].value_counts()
            features['market_u2i'] = subdomain_counts.get('u2i', 0)
            features['market_search'] = subdomain_counts.get('search', 0)
            features['market_catalog'] = subdomain_counts.get('catalog', 0)
            
            # Новые фичи для расширенной классификации
            features['engagement_ratio'] = features['market_clicks'] / max(1, features['market_views'])
            features['diversity_ratio'] = features['market_unique_items'] / max(1, features['market_events'])
            
            # Анализ интересов по item_id (упрощенный)
            items = user_data['item_id'].astype(str)
            features['tech_interest'] = items.str.contains('phone|mac|samsung|техник', case=False, na=False).sum()
            features['home_interest'] = items.str.contains('home|house|мебель|кухн', case=False, na=False).sum()
            features['sports_interest'] = items.str.contains('sport|спорт|фитнес', case=False, na=False).sum()
            
            # Нормализуем интересы
            total_interest = features['tech_interest'] + features['home_interest'] + features['sports_interest']
            if total_interest > 0:
                features['tech_interest_ratio'] = features['tech_interest'] / total_interest
                features['home_interest_ratio'] = features['home_interest'] / total_interest
                features['sports_interest_ratio'] = features['sports_interest'] / total_interest
            else:
                features['tech_interest_ratio'] = 0
                features['home_interest_ratio'] = 0
                features['sports_interest_ratio'] = 0
    
    except Exception as e:
        # Заполняем значения по умолчанию
        features.update({
            'market_events': 0, 'market_unique_items': 0, 'market_views': 0,
            'market_clicks': 0, 'market_likes': 0, 'market_u2i': 0,
            'market_search': 0, 'market_catalog': 0, 'engagement_ratio': 0,
            'diversity_ratio': 0, 'tech_interest_ratio': 0, 'home_interest_ratio': 0,
            'sports_interest_ratio': 0
        })
    
    return features

def get_offers_features_enhanced(user_id, base_path):
    """Расширенные фичи из offers"""
    features = {}
    try:
        events_path = f'{base_path}/offers/events'
        files = os.listdir(events_path)[:2]
        
        all_user_events = []
        for file in files:
            df = pd.read_parquet(f'{events_path}/{file}')
            user_events = df[df['user_id'] == user_id]
            all_user_events.append(user_events)
        
        if all_user_events:
            user_data = pd.concat(all_user_events, ignore_index=True)
            
            features['offers_seen'] = len(user_data)
            features['offers_unique'] = user_data['item_id'].nunique()
            
            action_counts = user_data['action_type'].value_counts()
            features['offers_seen_count'] = action_counts.get('seen', 0)
            features['offers_shown'] = action_counts.get('offer_shown', 0)
            features['offers_redirect'] = action_counts.get('redirect_to_partner', 0)
            features['offers_liked'] = action_counts.get('like', 0)
            features['offers_engagement'] = features['offers_shown'] + features['offers_redirect'] + features['offers_liked']
            
            # Новые метрики вовлеченности
            features['offers_engagement_ratio'] = features['offers_engagement'] / max(1, features['offers_seen'])
            features['offers_response_rate'] = features['offers_redirect'] / max(1, features['offers_shown'])
    
    except Exception as e:
        features.update({
            'offers_seen': 0, 'offers_unique': 0, 'offers_seen_count': 0,
            'offers_shown': 0, 'offers_redirect': 0, 'offers_liked': 0,
            'offers_engagement': 0, 'offers_engagement_ratio': 0, 'offers_response_rate': 0
        })
    
    return features

def get_retail_features_enhanced(user_id, base_path):
    """Фичи из retail"""
    features = {}
    try:
        events_path = f'{base_path}/retail/events'
        files = os.listdir(events_path)[:1]
        
        user_events = []
        for file in files:
            df = pd.read_parquet(f'{events_path}/{file}')
            user_df = df[df['user_id'] == user_id]
            user_events.append(user_df)
        
        if user_events:
            user_data = pd.concat(user_events, ignore_index=True)
            
            features['retail_events'] = len(user_data)
            features['retail_unique_items'] = user_data['item_id'].nunique()
            
            action_counts = user_data['action_type'].value_counts()
            features['retail_views'] = action_counts.get('view', 0)
            features['retail_cart_adds'] = action_counts.get('added-to-cart', 0)
            
            # Показатель покупательской активности
            features['retail_purchase_intent'] = features['retail_cart_adds'] / max(1, features['retail_views'])
    
    except Exception as e:
        features.update({
            'retail_events': 0, 'retail_unique_items': 0, 'retail_views': 0,
            'retail_cart_adds': 0, 'retail_purchase_intent': 0
        })
    
    return features

def create_enhanced_target(features):
    """Создаем расширенную целевую переменную с 10 категориями"""
    print("🎯 Создаем 10 категорий продуктов...")
    
    # Сбрасываем все значения
    features['target_product'] = 'savings_account'  # значение по умолчанию
    
    # СЛОЖНАЯ ЛОГИКА ДЛЯ 10 КАТЕГОРИЙ:
    conditions = [
        # 1. ПОТРЕБИТЕЛЬСКИЙ КРЕДИТ - высокая активность + вовлеченность
        (features['market_events'] > 80) & (features['offers_engagement'] > 8),
        
        # 2. РЕФИНАНСИРОВАНИЕ - средняя активность + высокая вовлеченность
        (features['market_events'] > 50) & (features['offers_engagement_ratio'] > 0.3),
        
        # 3. ИПОТЕКА - интерес к товарам для дома
        (features['home_interest_ratio'] > 0.6) & (features['market_events'] > 30),
        
        # 4. ПРЕМИУМ КАРТА - высокая активность + премиум поведение
        (features['market_events'] > 100) & (features['engagement_ratio'] > 0.1),
        
        # 5. КРЕДИТНАЯ КАРТА 180 - активные покупки + техника
        (features['tech_interest_ratio'] > 0.5) & (features['market_clicks'] > 10),
        
        # 6. ЗАРПЛАТНАЯ КАРТА - стабильная умеренная активность
        (features['market_events'].between(30, 100)) & (features['diversity_ratio'] > 0.3),
        
        # 7. СПОРТИВНАЯ КАРТА - интерес к спорту
        (features['sports_interest_ratio'] > 0.4) & (features['market_events'] > 20),
        
        # 8. ПЕНСИОННАЯ КАРТА - низкая активность
        (features['market_events'] < 20) & (features['offers_seen'] < 5),
        
        # 9. ВКЛАД - умеренная активность + низкая вовлеченность
        (features['market_events'].between(20, 60)) & (features['offers_engagement_ratio'] < 0.1),
    ]
    
    choices = [
        'consumer_loan',    # 1
        'refinancing',      # 2  
        'mortgage',         # 3
        'premium_card',     # 4
        'credit_card_180',  # 5
        'salary_card',      # 6
        'sports_card',      # 7
        'pension_card',     # 8
        'deposit_profitable' # 9
    ]
    
    # 10. СБЕРЕГАТЕЛЬНЫЙ СЧЕТ - значение по умолчанию (не включаем в conditions)
    features['target_product'] = np.select(conditions, choices, default='savings_account')
    
    return features

if __name__ == "__main__":
    create_enhanced_features()