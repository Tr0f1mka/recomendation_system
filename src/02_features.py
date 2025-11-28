# src/02_features_split.py
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from sklearn.model_selection import train_test_split
import pickle

def create_split_features():
    """Создаем фичи с разделением на train/test"""
    print("🎯 СОЗДАЕМ ФИЧИ С РАЗДЕЛЕНИЕМ НА TRAIN/TEST...")
    
    base_path = 'data/dataset/small'
    
    # 1. Загружаем ВСЕХ пользователей из marketplace
    print("📥 Загружаем всех пользователей...")
    all_users = load_all_users(base_path)
    print(f"👥 Всего пользователей: {len(all_users)}")
    
    # 2. Разделяем пользователей на train/test
    train_users, test_users = train_test_split(
        all_users, test_size=0.2, random_state=42
    )
    
    print(f"📚 Train пользователей: {len(train_users)}")
    print(f"🧪 Test пользователей: {len(test_users)}")
    
    # 3. Создаем фичи РАЗДЕЛЬНО
    print("\n🔨 Создаем фичи для TRAIN...")
    train_features = create_features_for_users(train_users, base_path)
    
    print("\n🔨 Создаем фичи для TEST...")
    test_features = create_features_for_users(test_users, base_path)
    
    # 4. Создаем целевую переменную
    train_features = create_target_variable(train_features)
    test_features = create_target_variable(test_features)
    
    # 5. Сохраняем раздельно
    train_features.to_parquet('train_features.pq', index=False)
    test_features.to_parquet('test_features.pq', index=False)
    
    # Сохраняем списки пользователей
    with open('train_users.pkl', 'wb') as f:
        pickle.dump(train_users, f)
    with open('test_users.pkl', 'wb') as f:
        pickle.dump(test_users, f)
    
    print(f"\n💾 СОХРАНЕНО:")
    print(f"📚 Train: {len(train_features)} пользователей")
    print(f"🧪 Test: {len(test_features)} пользователей")
    print(f"📊 Распределение в train: {train_features['target_product'].value_counts().to_dict()}")
    print(f"📊 Распределение в test: {test_features['target_product'].value_counts().to_dict()}")
    
    return train_features, test_features

def load_all_users(base_path, max_users=5000):
    """Загружаем всех пользователей из нескольких файлов"""
    users = set()
    events_path = f'{base_path}/marketplace/events'
    files = os.listdir(events_path)[:3]  # 3 файла для хорошего покрытия
    
    for file in tqdm(files, desc="Загрузка пользователей"):
        try:
            df = pd.read_parquet(f'{events_path}/{file}')
            if 'user_id' in df.columns:
                users.update(df['user_id'].unique())
        except Exception as e:
            print(f"⚠️ Ошибка загрузки {file}: {e}")
    
    return list(users)[:max_users]

def create_features_for_users(user_list, base_path):
    """Создаем фичи для списка пользователей"""
    features_data = []
    
    for user_id in tqdm(user_list, desc="Создание фичей"):
        try:
            user_features = {'user_id': user_id}
            
            # Marketplace фичи
            market_features = get_marketplace_features(user_id, base_path)
            user_features.update(market_features)
            
            # Offers фичи
            offers_features = get_offers_features(user_id, base_path)
            user_features.update(offers_features)
            
            features_data.append(user_features)
            
        except Exception:
            continue
    
    return pd.DataFrame(features_data)

def get_marketplace_features(user_id, base_path):
    """Фичи из marketplace"""
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
            
            features['market_events'] = len(user_data)
            features['market_unique_items'] = user_data['item_id'].nunique()
            
            action_counts = user_data['action_type'].value_counts()
            features['market_views'] = action_counts.get('view', 0)
            features['market_clicks'] = action_counts.get('click', 0) + action_counts.get('clickout', 0)
            features['market_likes'] = action_counts.get('like', 0)
            
            subdomain_counts = user_data['subdomain'].value_counts()
            features['market_u2i'] = subdomain_counts.get('u2i', 0)
            features['market_search'] = subdomain_counts.get('search', 0)
            
            # Engagement ratio
            features['market_engagement_ratio'] = features['market_clicks'] / max(1, features['market_views'])
    
    except Exception:
        features.update({
            'market_events': 0, 'market_unique_items': 0, 'market_views': 0,
            'market_clicks': 0, 'market_likes': 0, 'market_u2i': 0,
            'market_search': 0, 'market_engagement_ratio': 0
        })
    
    return features

def get_offers_features(user_id, base_path):
    """Фичи из offers"""
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
    
    except Exception:
        features.update({
            'offers_seen': 0, 'offers_unique': 0, 'offers_seen_count': 0,
            'offers_shown': 0, 'offers_redirect': 0, 'offers_liked': 0,
            'offers_engagement': 0
        })
    
    return features

def create_target_variable(features):
    """Создаем целевую переменную"""
    # Более сложная логика чтобы избежать переобучения
    conditions = [
        # Кредитные продукты - высокая вовлеченность
        (features['offers_engagement'] > 8) & (features['market_events'] > 80),
        
        # Дебетовые продукты - средняя активность  
        (features['offers_seen'] > 15) & (features['market_events'] > 30),
        
        # Сбережения - низкая активность
        (features['offers_seen'] <= 10)
    ]
    
    choices = ['credit_card', 'debit_card', 'savings']
    features['target_product'] = np.select(conditions, choices, default='debit_card')
    
    return features

if __name__ == "__main__":
    train_features, test_features = create_split_features()