# src/01_eda_complete.py
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

def analyze_all_domains():
    print("🔍 ПОЛНЫЙ АНАЛИЗ ВСЕХ ДАННЫХ...")
    
    base_path = 'data/dataset/small'
    domains = ['marketplace', 'offers', 'retail', 'reviews']
    
    for domain in domains:
        print(f"\n{'='*50}")
        print(f"📊 АНАЛИЗ: {domain.upper()}")
        print(f"{'='*50}")
        
        try:
            if domain == 'reviews':
                # Reviews - это отдельные файлы, а не папка
                review_files = [f for f in os.listdir(base_path) if f.startswith('review') and f.endswith('.pq')]
                if review_files:
                    sample_file = review_files[0]
                    data = pd.read_parquet(f'{base_path}/{sample_file}')
                    print(f"✅ Reviews загружены из {sample_file}")
                else:
                    print("❌ Файлы reviews не найдены")
                    continue
            else:
                # Для marketplace, offers, retail
                events_path = f'{base_path}/{domain}/events'
                if os.path.exists(events_path):
                    files = os.listdir(events_path)
                    sample_file = files[0]
                    data = pd.read_parquet(f'{events_path}/{sample_file}')
                    print(f"✅ {domain} события загружены из {sample_file}")
                    
                    # Загружаем items если есть
                    items_path = f'{base_path}/{domain}/items.pq'
                    if os.path.exists(items_path):
                        items = pd.read_parquet(items_path)
                        print(f"✅ {domain} товары: {len(items)} записей")
                        print(f"   Колонки items: {items.columns.tolist()}")
                else:
                    print(f"❌ Путь {events_path} не существует")
                    continue
            
            # Общая статистика
            print(f"📈 Размер данных: {len(data)} строк")
            print(f"📋 Колонки: {data.columns.tolist()}")
            
            # Уникальные значения
            for col in data.columns:
                if col in ['user_id', 'item_id']:
                    print(f"   Уникальных {col}: {data[col].nunique()}")
            
            # Статистика по колонкам
            for col in data.columns:
                if data[col].dtype == 'object':
                    value_counts = data[col].value_counts()
                    print(f"   {col}: {dict(value_counts.head())}")  # топ-5 значений
            
            # Примеры данных
            print(f"👀 Примеры данных ({domain}):")
            print(data.head(3))
            
        except Exception as e:
            print(f"❌ Ошибка анализа {domain}: {e}")

def compare_user_overlap():
    print(f"\n{'='*50}")
    print("🔗 СРАВНЕНИЕ ПОЛЬЗОВАТЕЛЕЙ МЕЖДУ ДОМЕНАМИ")
    print(f"{'='*50}")
    
    base_path = 'data/dataset/small'
    domains = ['marketplace', 'offers', 'retail']
    
    user_sets = {}
    
    for domain in domains:
        try:
            events_path = f'{base_path}/{domain}/events'
            if os.path.exists(events_path):
                files = os.listdir(events_path)
                sample_file = files[0]
                data = pd.read_parquet(f'{events_path}/{sample_file}')
                
                if 'user_id' in data.columns:
                    users = set(data['user_id'].unique())
                    user_sets[domain] = users
                    print(f"👥 {domain}: {len(users)} пользователей")
                
        except Exception as e:
            print(f"❌ Ошибка {domain}: {e}")
    
    # Анализ пересечений
    if len(user_sets) >= 2:
        domains_list = list(user_sets.keys())
        print(f"\n📊 ПЕРЕСЕЧЕНИЯ ПОЛЬЗОВАТЕЛЕЙ:")
        
        for i in range(len(domains_list)):
            for j in range(i+1, len(domains_list)):
                domain1, domain2 = domains_list[i], domains_list[j]
                intersection = user_sets[domain1] & user_sets[domain2]
                print(f"   {domain1} ∩ {domain2}: {len(intersection)} пользователей")

if __name__ == "__main__":
    analyze_all_domains()
    compare_user_overlap()
    
    print(f"\n{'='*50}")
    print("🎯 ВЫВОДЫ ДЛЯ РЕКОМЕНДАТЕЛЬНОЙ СИСТЕМЫ:")
    print("=" * 50)
    print("1. Marketplace - основное поведение (просмотры, клики)")
    print("2. Offers - реакции на банковские предложения") 
    print("3. Retail - дополнительное покупательское поведение")
    print("4. Reviews - отзывы (если есть)")
    print("5. Анализ пересечений покажет общих пользователей")