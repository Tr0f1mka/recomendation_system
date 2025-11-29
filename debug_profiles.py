# debug_profiles.py - разместите в корне проекта
import sys
from pathlib import Path
import polars as pl

# Добавляем корневую директорию в путь
sys.path.append(str(Path(__file__).parent))

from src.data_processor import DataProcessor
from src.user_profiler import UserProfiler

def debug_profile_creation():
    """Диагностика создания профилей"""
    print("🔍 ДИАГНОСТИКА СОЗДАНИЯ ПРОФИЛЕЙ")
    print("=" * 50)
    
    # Загрузка данных
    processor = DataProcessor()
    data = processor.load_all_data(sample_fraction=0.01)
    
    if not data:
        print("❌ Не удалось загрузить данные")
        return
    
    print("\n📊 АНАЛИЗ ДАННЫХ:")
    print(f"👥 Пользователи: {data['users'].shape}")
    print(f"🛍️ Товары: {data['retail_items'].shape}")
    print(f"📊 События: {data['retail_events'].shape}")
    
    # Проверим структуру данных
    print(f"\n🔍 СТРУКТУРА ДАННЫХ:")
    print(f"Колонки users: {data['users'].columns}")
    print(f"Колонки events: {data['retail_events'].columns}")
    print(f"Колонки items: {data['retail_items'].columns}")
    
    # Проверим пересечение пользователей
    try:
        event_users = data['retail_events']['user_id'].unique()
        all_users = data['users']['user_id']
        
        print(f"\n🔗 ПЕРЕСЕЧЕНИЕ ПОЛЬЗОВАТЕЛЕЙ:")
        print(f"   Всего пользователей: {all_users.len()}")
        print(f"   Пользователей в событиях: {event_users.len()}")
        
        # Найдем общих пользователей
        common_users = set(all_users).intersection(set(event_users))
        print(f"   Общих пользователей: {len(common_users)}")
        
        if len(common_users) == 0:
            print("   ⚠️ НЕТ ОБЩИХ ПОЛЬЗОВАТЕЛЕЙ! Это основная проблема.")
            print("   Возможные причины:")
            print("   - Разные наборы user_id в users.pq и events")
            print("   - Семплирование затронуло разных пользователей")
            print("   - Данные из разных источников")
            
    except Exception as e:
        print(f"❌ Ошибка при анализе пользователей: {e}")
    
    # Проверим несколько конкретных пользователей
    print(f"\n👤 ПРОВЕРКА КОНКРЕТНЫХ ПОЛЬЗОВАТЕЛЕЙ:")
    
    # Возьмем первых 5 пользователей из users
    sample_user_ids = data['users']['user_id'].head(5).to_list()
    print(f"   Пример user_id из users: {sample_user_ids}")
    
    # Проверим есть ли они в событиях
    for user_id in sample_user_ids:
        user_events = data['retail_events'].filter(pl.col('user_id') == user_id)
        print(f"   User {user_id}: {user_events.height} событий")
    
    # Проверим первые 5 user_id из событий
    sample_event_users = data['retail_events']['user_id'].head(5).to_list()
    print(f"   Пример user_id из events: {sample_event_users}")
    
    # Проверим структуру событий
    print(f"\n📋 СТРУКТУРА СОБЫТИЙ:")
    if data['retail_events'].height > 0:
        sample_event = data['retail_events'].head(1)
        print(f"   Пример события: {sample_event.row(0)}")
        
        # Проверим action_type
        if 'action_type' in data['retail_events'].columns:
            action_counts = data['retail_events']['action_type'].value_counts()
            print(f"   Типы действий: {action_counts.to_dicts()}")

if __name__ == "__main__":
    debug_profile_creation()