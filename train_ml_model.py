from src.data_processor import DataProcessor
from src.recommendation_engine import AdvancedRecommendationEngine
from src.category_discovery import CategoryDiscoverer
from src.product_matcher import ProductMatcher
import json
import random

def generate_training_data():
    """Генерация тренировочных данных для ML на основе реальных данных"""
    print("🤖 Генерация тренировочных данных для ML...")
    
    # Загружаем реальные данные
    processor = DataProcessor()
    data = processor.load_all_data(sample_fraction=0.05)
    
    if not data or 'retail_items' not in data:
        print("❌ Нет данных для обучения")
        return []
    
    # Исправляем цены
    data['retail_items'] = processor.fix_log_prices(data['retail_items'])
    
    # Обнаружение категорий
    discoverer = CategoryDiscoverer()
    categories = discoverer.discover_categories_from_data(data['retail_items'])
    
    # Создаем движок
    engine = AdvancedRecommendationEngine(discovered_categories=categories)
    
    # Создаем пользовательские профили
    print("👤 Создание пользовательских профилей для обучения...")
    user_profiles = engine.user_profiler.create_user_profiles(
        data['users'], data['retail_events'], data['retail_items']
    )
    
    if user_profiles.height == 0:
        print("❌ Не удалось создать профили для обучения")
        return []
    
    # Генерируем тренировочные данные
    training_data = []
    
    # Берем банковские продукты для обучения
    from config.products import BANK_PRODUCTS
    all_products = []
    for product_type, products in BANK_PRODUCTS.items():
        for product in products:
            product['product_type'] = product_type
            all_products.append(product)
    
    print(f"🔄 Генерация примеров для {user_profiles.height} пользователей и {len(all_products)} продуктов...")
    
    for user_row in user_profiles.head(200).iter_rows(named=True):  # Ограничиваем для скорости
        for product in all_products:
            # Создаем реалистичные примеры с разной конверсией
            user_total_spent = user_row.get('total_spent', 0)
            user_spending_level = user_row.get('spending_level', 'unknown')
            product_business_value = product.get('business_value', 0.5)
            
            # Логика определения конверсии на основе данных
            if user_spending_level in ['high', 'very_high'] and product_business_value > 0.7:
                conversion_rate = random.uniform(0.6, 0.9)
            elif user_spending_level == 'medium' and product_business_value > 0.5:
                conversion_rate = random.uniform(0.4, 0.7)
            else:
                conversion_rate = random.uniform(0.1, 0.4)
            
            training_example = {
                'user_profile': {
                    'total_spent': user_row.get('total_spent', 0),
                    'avg_transaction_value': user_row.get('avg_transaction_value', 0),
                    'spending_level': user_row.get('spending_level', 'unknown'),
                    'interaction_frequency': user_row.get('interaction_frequency', 'unknown'),
                    'category_diversity': user_row.get('category_diversity', 0)
                },
                'product': {
                    'id': product['id'],
                    'name': product['name'],
                    'business_value': product.get('business_value', 0.5)
                },
                'conversion_rate': conversion_rate
            }
            training_data.append(training_example)
    
    print(f"✅ Сгенерировано {len(training_data)} тренировочных примеров")
    return training_data

def main():
    print("🚀 ЗАПУСК ОБУЧЕНИЯ ML МОДЕЛИ")
    print("=" * 40)
    
    # Генерируем тренировочные данные
    training_data = generate_training_data()
    
    if not training_data:
        print("❌ Не удалось сгенерировать тренировочные данные")
        return
    
    # Сохраняем данные
    with open('training_data.json', 'w', encoding='utf-8') as f:
        json.dump(training_data, f, ensure_ascii=False, indent=2)
    
    print("💾 Тренировочные данные сохранены в training_data.json")
    
    # Обучаем ML модель
    print("🧠 Обучение ML модели...")
    
    # Загружаем данные для инициализации категорий
    processor = DataProcessor()
    data = processor.load_all_data(sample_fraction=0.01)
    categories = CategoryDiscoverer().discover_categories_from_data(data['retail_items'])
    
    # Создаем matcher и обучаем модель
    matcher = ProductMatcher(discovered_categories=categories)
    
    try:
        matcher.ml_enhancer.train_model(training_data)
        print("✅ ML модель успешно обучена!")
        
    except Exception as e:
        print(f"❌ Ошибка обучения ML модели: {e}")

if __name__ == "__main__":
    main()